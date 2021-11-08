package neofs

import (
	"bytes"
	"context"
	"fmt"
	"hash"
	"io"
	"strings"

	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	cid "github.com/nspcc-dev/neofs-api-go/pkg/container/id"
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-sdk-go/pool"
	"github.com/restic/restic/internal/backend"
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
		address *object.Address
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

	containerID, err := getContainerID(ctx, p, cfg.Container, cfg.Policy)
	if err != nil {
		return nil, err
	}
	fmt.Println("container repo", containerID.String())

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
	name := getName(h)
	opts := object.NewSearchFilters()
	opts.AddRootFilter()
	opts.AddFilter(object.AttributeFileName, name, object.MatchStringEqual)

	p := new(client.SearchObjectParams).WithContainerID(b.cnrID).WithSearchFilters(opts)
	ids, err := b.client.SearchObject(ctx, p)
	return len(ids) != 0, err
}

func (b *Backend) Remove(ctx context.Context, h restic.Handle) error {
	objInfo, err := b.stat(ctx, h)
	if err != nil {
		return err
	}

	p := new(client.DeleteObjectParams).WithAddress(objInfo.address)
	return b.client.DeleteObject(ctx, p)
}

func (b *Backend) Close() error {
	b.client.Close()
	return nil
}

func (b *Backend) Save(ctx context.Context, h restic.Handle, rd restic.RewindReader) error {
	name := getName(h)
	rawObj := formRawObject(b.client.OwnerID(), b.cnrID, name, map[string]string{attrResticType: string(h.Type)})
	p := new(client.PutObjectParams).WithObject(rawObj.Object()).WithPayloadReader(rd)

	_, err := b.client.PutObject(ctx, p)
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

	rang := object.NewRange()
	rang.SetOffset(uint64(offset))
	if length != 0 {
		rang.SetLength(uint64(length))
	} else {
		rang.SetLength(uint64(objInfo.Size - offset))
	}

	ops := new(client.RangeDataParams).WithAddress(objInfo.address).WithRange(rang)
	data, err := b.client.ObjectPayloadRangeData(ctx, ops)
	if err != nil {
		return nil, err
	}

	return &BuffCloser{Reader: bytes.NewReader(data)}, nil
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

	addr := newAddress(b.cnrID, ids[0])
	hp := new(client.ObjectHeaderParams).WithAddress(addr)
	obj, err := b.client.GetObjectHeader(ctx, hp)
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

	for _, oid := range ids {
		addr := newAddress(b.cnrID, oid)
		hp := new(client.ObjectHeaderParams).WithAddress(addr)
		obj, err := b.client.GetObjectHeader(ctx, hp)
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

func (b *Backend) Connections() uint {
	// TODO(@KirillovDenis): use appropriate value
	return 2
}

func (b *Backend) HasAtomicReplace() bool {
	return false
}

func (b *Backend) IsNotExist(err error) bool {
	return strings.Contains(err.Error(), "not found")
}

func (b *Backend) Delete(ctx context.Context) error {
	return b.client.DeleteContainer(ctx, b.cnrID)
}

func (b *Backend) searchObjects(ctx context.Context, filters map[string]string) ([]*object.ID, error) {
	opts := object.NewSearchFilters()
	opts.AddRootFilter()

	for key, val := range filters {
		opts.AddFilter(key, val, object.MatchStringEqual)
	}

	p := new(client.SearchObjectParams).WithContainerID(b.cnrID).WithSearchFilters(opts)
	return b.client.SearchObject(ctx, p)
}

func getName(h restic.Handle) string {
	name := h.Name
	if h.Type == restic.ConfigFile {
		name = "config"
	}
	return name
}
