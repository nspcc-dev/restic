package neofs

import (
	"context"
	"fmt"
	"hash"
	"io"
	"strings"

	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/pool"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/backend/sema"
	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/restic"
)

type (
	// Backend stores data on a neofs storage.
	Backend struct {
		client *pool.Pool
		owner  *user.ID
		cnrID  cid.ID

		sem         sema.Semaphore
		connections uint
		compression bool
	}

	// ObjInfo represents inner file info.
	ObjInfo struct {
		restic.FileInfo
		address oid.Address
	}
)

const (
	attrResticType = "restic-type"
	compressedType = "application/zsd"
)

func Open(ctx context.Context, cfg Config) (restic.Backend, error) {
	return open(ctx, cfg)
}

func Create(ctx context.Context, cfg Config) (restic.Backend, error) {
	return open(ctx, cfg)
}

func open(ctx context.Context, cfg Config) (restic.Backend, error) {
	sem, err := sema.New(cfg.Connections)
	if err != nil {
		return nil, err
	}

	acc, err := getAccount(cfg)
	if err != nil {
		return nil, err
	}

	var owner user.ID
	user.IDFromKey(&owner, acc.PrivateKey().PrivateKey.PublicKey)

	p, err := createPool(ctx, acc, cfg)
	if err != nil {
		return nil, err
	}

	containerID, err := resolveContainerID(cfg.RPCEndpoint, cfg.Container)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve container id: %w", err)
	}
	debug.Log("container repo: %s", containerID.String())

	return &Backend{
		client:      p,
		owner:       &owner,
		cnrID:       containerID,
		sem:         sem,
		connections: cfg.Connections,
		compression: cfg.Compression,
	}, nil
}

func (b *Backend) Location() string {
	return b.cnrID.String()
}

func (b *Backend) Hasher() hash.Hash {
	return nil // nil is valid value
}

func (b *Backend) Test(ctx context.Context, h restic.Handle) (bool, error) {
	filters := object.NewSearchFilters()
	filters.AddRootFilter()
	filters.AddFilter(object.AttributeFilePath, getName(h), object.MatchStringEqual)

	var prmSearch pool.PrmObjectSearch
	prmSearch.SetContainerID(b.cnrID)
	prmSearch.SetFilters(filters)

	b.sem.GetToken()
	defer b.sem.ReleaseToken()

	res, err := b.client.SearchObjects(ctx, prmSearch)
	if err != nil {
		return false, fmt.Errorf("search objects: %w", err)
	}

	defer res.Close()

	var has bool

	err = res.Iterate(func(id oid.ID) bool {
		has = true
		return true
	})
	if err != nil {
		return false, fmt.Errorf("iterate objects: %w", err)
	}

	return has, nil
}

func (b *Backend) Remove(ctx context.Context, h restic.Handle) error {
	b.sem.GetToken()
	defer b.sem.ReleaseToken()

	objInfo, err := b.stat(ctx, h)
	if err != nil {
		return err
	}

	var prm pool.PrmObjectDelete
	prm.SetAddress(objInfo.address)

	return b.client.DeleteObject(ctx, prm)
}

func (b *Backend) Close() error {
	b.client.Close()
	return nil
}

func (b *Backend) Save(ctx context.Context, h restic.Handle, rd restic.RewindReader) error {
	name := getName(h)

	extraAttributes := map[string]string{attrResticType: string(h.Type)}
	if b.compression {
		extraAttributes[object.AttributeContentType] = compressedType
	}

	obj := formRawObject(b.owner, b.cnrID, name, extraAttributes)

	var prm pool.PrmObjectPut
	prm.SetHeader(*obj)
	prm.SetPayload(rd)

	b.sem.GetToken()
	defer b.sem.ReleaseToken()

	_, err := b.client.PutObject(ctx, prm)
	return err
}

func (b *Backend) Load(ctx context.Context, h restic.Handle, length int, offset int64, fn func(rd io.Reader) error) error {
	return backend.DefaultLoad(ctx, h, length, offset, b.openReader, fn)
}

func (b *Backend) openReader(ctx context.Context, h restic.Handle, length int, offset int64) (io.ReadCloser, error) {
	b.sem.GetToken()
	ctx, cancel := context.WithCancel(ctx)

	objInfo, err := b.stat(ctx, h)
	if err != nil {
		cancel()
		b.sem.ReleaseToken()
		return nil, err
	}

	ln := uint64(length)
	if ln == 0 {
		ln = uint64(objInfo.Size - offset)
	}

	var prm pool.PrmObjectRange
	prm.SetAddress(objInfo.address)
	prm.SetOffset(uint64(offset))
	prm.SetLength(ln)

	res, err := b.client.ObjectRange(ctx, prm)
	if err != nil {
		cancel()
		b.sem.ReleaseToken()
		return nil, err
	}

	return b.sem.ReleaseTokenOnClose(&res, cancel), nil
}

func (b *Backend) stat(ctx context.Context, h restic.Handle) (*ObjInfo, error) {
	name := getName(h)
	filters := object.NewSearchFilters()
	filters.AddRootFilter()
	filters.AddFilter(object.AttributeFilePath, name, object.MatchStringEqual)
	filters.AddFilter(attrResticType, string(h.Type), object.MatchStringEqual)

	var prmSearch pool.PrmObjectSearch
	prmSearch.SetContainerID(b.cnrID)
	prmSearch.SetFilters(filters)

	res, err := b.client.SearchObjects(ctx, prmSearch)
	if err != nil {
		return nil, fmt.Errorf("search objects: %w", err)
	}

	defer res.Close()

	var objID oid.ID
	var found bool

	var inErr error
	err = res.Iterate(func(id oid.ID) bool {
		if found {
			inErr = fmt.Errorf("found more than one object for file: '%s'", name)
			return true
		}
		objID = id
		found = true
		return false
	})
	if err == nil {
		err = inErr
	}
	if err != nil {
		return nil, fmt.Errorf("iterate objects: %w", err)
	}

	if !found {
		return nil, fmt.Errorf("not found file: '%s'", name)
	}

	addr := newAddress(b.cnrID, objID)
	var prm pool.PrmObjectHead
	prm.SetAddress(addr)

	obj, err := b.client.HeadObject(ctx, prm)
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
	b.sem.GetToken()
	defer b.sem.ReleaseToken()

	objInfo, err := b.stat(ctx, h)
	if err != nil {
		return restic.FileInfo{}, err
	}
	return objInfo.FileInfo, nil
}

func (b *Backend) List(ctx context.Context, t restic.FileType, fn func(restic.FileInfo) error) error {
	filters := object.NewSearchFilters()
	filters.AddRootFilter()
	filters.AddFilter(attrResticType, string(t), object.MatchStringEqual)

	var prmSearch pool.PrmObjectSearch
	prmSearch.SetContainerID(b.cnrID)
	prmSearch.SetFilters(filters)

	b.sem.GetToken()
	defer b.sem.ReleaseToken()

	res, err := b.client.SearchObjects(ctx, prmSearch)
	if err != nil {
		return fmt.Errorf("search objects: %w", err)
	}

	defer res.Close()

	var addr oid.Address
	addr.SetContainer(b.cnrID)

	var inErr error
	err = res.Iterate(func(id oid.ID) bool {
		addr.SetObject(id)

		var prm pool.PrmObjectHead
		prm.SetAddress(addr)

		obj, err := b.client.HeadObject(ctx, prm)
		if err != nil {
			inErr = fmt.Errorf("head object: %w", err)
			return true
		}

		fileInfo := restic.FileInfo{
			Size: int64(obj.PayloadSize()),
			Name: getFilePathAttr(obj),
		}
		if err = fn(fileInfo); err != nil {
			inErr = fmt.Errorf("handle fileInfo: %w", err)
			return true
		}

		return false
	})
	if err == nil {
		err = inErr
	}
	if err != nil {
		return fmt.Errorf("iterate objects: %w", err)
	}

	return nil
}

func (b *Backend) Connections() uint {
	return b.connections
}

func (b *Backend) HasAtomicReplace() bool {
	return false
}

func (b *Backend) IsNotExist(err error) bool {
	return strings.Contains(err.Error(), "not found")
}

func (b *Backend) Delete(ctx context.Context) error {
	var prm pool.PrmContainerDelete
	prm.SetContainerID(b.cnrID)

	b.sem.GetToken()
	defer b.sem.ReleaseToken()

	if err := b.client.DeleteContainer(ctx, prm); err != nil {
		return fmt.Errorf("delete container: %w", err)
	}

	return nil
}

func getName(h restic.Handle) string {
	name := h.Name
	if h.Type == restic.ConfigFile {
		name = "config"
	}
	return name
}
