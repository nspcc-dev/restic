package neofs

import (
	"context"
	"fmt"
	"hash"
	"io"
	"strings"

	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/nspcc-dev/neofs-sdk-go/object/address"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/pool"
	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/restic"
)

type (
	// Backend stores data on a neofs storage.
	Backend struct {
		client pool.Pool
		cnrID  *cid.ID
	}

	// ObjInfo represents inner file info.
	ObjInfo struct {
		restic.FileInfo
		address *address.Address
	}
)

const attrResticType = "restic-type"

func Open(ctx context.Context, cfg Config) (restic.Backend, error) {
	return open(ctx, cfg)
}

func Create(ctx context.Context, cfg Config) (restic.Backend, error) {
	return open(ctx, cfg)
}

func open(ctx context.Context, cfg Config) (restic.Backend, error) {
	p, err := createPool(ctx, cfg)
	if err != nil {
		return nil, err
	}

	containerID, err := getContainerID(ctx, p, cfg.Container)
	if err != nil {
		return nil, err
	}
	debug.Log("container repo: %s", containerID.String())

	return &Backend{
		client: p,
		cnrID:  containerID,
	}, nil
}

func (b *Backend) Location() string {
	return b.cnrID.String()
}

func (b *Backend) Hasher() hash.Hash {
	return nil // nil is valid value
}

func (b *Backend) Test(ctx context.Context, h restic.Handle) (bool, error) {
	filters := map[string]string{
		object.AttributeFileName: getName(h),
	}

	ids, err := b.searchObjects(ctx, filters)
	return len(ids) != 0, err
}

func (b *Backend) Remove(ctx context.Context, h restic.Handle) error {
	objInfo, err := b.stat(ctx, h)
	if err != nil {
		return err
	}

	return b.client.DeleteObject(ctx, *objInfo.address)
}

func (b *Backend) Close() error {
	b.client.Close()
	return nil
}

func (b *Backend) Save(ctx context.Context, h restic.Handle, rd restic.RewindReader) error {
	name := getName(h)
	rawObj := formRawObject(b.client.OwnerID(), b.cnrID, name, map[string]string{attrResticType: string(h.Type)})

	_, err := b.client.PutObject(ctx, *rawObj.Object(), rd)
	return err
}

func (b *Backend) Load(ctx context.Context, h restic.Handle, length int, offset int64, fn func(rd io.Reader) error) error {
	return backend.DefaultLoad(ctx, h, length, offset, b.openReader, fn)
}

func (b *Backend) openReader(ctx context.Context, h restic.Handle, length int, offset int64) (io.ReadCloser, error) {
	objInfo, err := b.stat(ctx, h)
	if err != nil {
		return nil, err
	}

	ln := uint64(length)
	if ln == 0 {
		ln = uint64(objInfo.Size - offset)
	}

	res, err := b.client.ObjectRange(ctx, *objInfo.address, uint64(offset), ln)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (b *Backend) stat(ctx context.Context, h restic.Handle) (*ObjInfo, error) {
	name := getName(h)
	filters := map[string]string{
		object.AttributeFileName: name,
		attrResticType:           string(h.Type),
	}

	ids, err := b.searchObjects(ctx, filters)
	if err != nil {
		return nil, err
	}
	if len(ids) != 1 {
		return nil, fmt.Errorf("not found exactly one file: %s", name)
	}

	addr := newAddress(b.cnrID, &ids[0])
	obj, err := b.client.HeadObject(ctx, *addr)
	if err != nil {
		return nil, err
	}

	return &ObjInfo{
		FileInfo: restic.FileInfo{
			Name: name,
			Size: int64(obj.PayloadSize()),
		},
		address: addr,
	}, nil
}

func (b *Backend) Stat(ctx context.Context, h restic.Handle) (restic.FileInfo, error) {
	objInfo, err := b.stat(ctx, h)
	if err != nil {
		return restic.FileInfo{}, err
	}
	return objInfo.FileInfo, nil
}

func (b *Backend) List(ctx context.Context, t restic.FileType, fn func(restic.FileInfo) error) error {
	ids, err := b.searchObjects(ctx, map[string]string{attrResticType: string(t)})
	if err != nil {
		return err
	}

	for _, id := range ids {
		addr := newAddress(b.cnrID, &id)
		obj, err := b.client.HeadObject(ctx, *addr)
		if err != nil {
			return err
		}

		fileInfo := restic.FileInfo{
			Size: int64(obj.PayloadSize()),
			Name: getNameAttr(obj),
		}
		if err = fn(fileInfo); err != nil {
			return err
		}
	}

	return nil
}

func (b *Backend) IsNotExist(err error) bool {
	return strings.Contains(err.Error(), "not found")
}

func (b *Backend) Delete(ctx context.Context) error {
	return b.client.DeleteContainer(ctx, b.cnrID)
}

func (b *Backend) searchObjects(ctx context.Context, filters map[string]string) ([]oid.ID, error) {
	opts := object.NewSearchFilters()
	opts.AddRootFilter()

	for key, val := range filters {
		opts.AddFilter(key, val, object.MatchStringEqual)
	}

	return searchObjects(ctx, b.client, b.cnrID, opts)
}

func getName(h restic.Handle) string {
	name := h.Name
	if h.Type == restic.ConfigFile {
		name = "config"
	}
	return name
}

func searchObjects(ctx context.Context, sdkPool pool.Pool, cnrID *cid.ID, filters object.SearchFilters) ([]oid.ID, error) {
	res, err := sdkPool.SearchObjects(ctx, *cnrID, filters)
	if err != nil {
		return nil, fmt.Errorf("init searching using client: %w", err)
	}

	defer res.Close()

	var num, read int
	buf := make([]oid.ID, 10)

	for {
		num, err = res.Read(buf[read:])
		if num > 0 {
			read += num
			buf = append(buf, oid.ID{})
			buf = buf[:cap(buf)]
		}

		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return nil, fmt.Errorf("couldn't read found objects: %w", err)
		}
	}

	return buf[:read], nil
}
