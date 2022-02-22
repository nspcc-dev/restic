package neofs

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/nspcc-dev/neo-go/cli/flags"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/nspcc-dev/neofs-sdk-go/object/address"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/owner"
	"github.com/nspcc-dev/neofs-sdk-go/pool"
)

// BuffCloser is wrapper to load files from neofs.
type BuffCloser struct {
	io.Reader
}

func (bc *BuffCloser) Close() error {
	return nil
}

func createPool(ctx context.Context, cfg Config) (pool.Pool, error) {
	acc, err := getAccount(cfg)
	if err != nil {
		return nil, err
	}

	pb := new(pool.Builder)
	pb.AddNode(cfg.Endpoint, 1, 1)

	opts := &pool.BuilderOptions{
		Key:                     &acc.PrivateKey().PrivateKey,
		NodeConnectionTimeout:   cfg.Timeout,
		NodeRequestTimeout:      cfg.Timeout,
		ClientRebalanceInterval: cfg.RebalanceInterval,
	}

	return pb.Build(ctx, opts)
}

func getAccount(cfg Config) (*wallet.Account, error) {
	w, err := wallet.NewWalletFromFile(cfg.Wallet)
	if err != nil {
		return nil, err
	}

	addr := w.GetChangeAddress()
	if cfg.Address != "" {
		addr, err = flags.ParseAddress(cfg.Address)
		if err != nil {
			return nil, fmt.Errorf("invalid address")
		}
	}
	acc := w.GetAccount(addr)
	err = acc.Decrypt(cfg.Password, w.Scrypt)
	if err != nil {
		return nil, err
	}

	return acc, nil
}

func getContainerID(ctx context.Context, client pool.Pool, container string) (*cid.ID, error) {
	cnrID := cid.New()
	if err := cnrID.Parse(container); err != nil {
		return findContainerID(ctx, client, container)
	}
	return cnrID, nil
}

func findContainerID(ctx context.Context, client pool.Pool, containerName string) (*cid.ID, error) {
	containerIDs, err := client.ListContainers(ctx, client.OwnerID())
	if err != nil {
		return nil, err
	}

	for _, cnrID := range containerIDs {
		cnr, err := client.GetContainer(ctx, cnrID)
		if err != nil {
			return nil, err
		}

		for _, attr := range cnr.Attributes() {
			if attr.Key() == container.AttributeName && attr.Value() == containerName {
				return cnrID, nil
			}
		}
	}

	return nil, fmt.Errorf("container '%s' not found", containerName)
}

func formRawObject(own *owner.ID, cnrID *cid.ID, name string, header map[string]string) *object.RawObject {
	attributes := make([]*object.Attribute, 0, 2+len(header))
	filename := object.NewAttribute()
	filename.SetKey(object.AttributeFileName)
	filename.SetValue(name)

	createdAt := object.NewAttribute()
	createdAt.SetKey(object.AttributeTimestamp)
	createdAt.SetValue(strconv.FormatInt(time.Now().UTC().Unix(), 10))

	attributes = append(attributes, filename, createdAt)

	for key, val := range header {
		attr := object.NewAttribute()
		attr.SetKey(key)
		attr.SetValue(val)
		attributes = append(attributes, attr)
	}

	raw := object.NewRaw()
	raw.SetOwnerID(own)
	raw.SetContainerID(cnrID)
	raw.SetAttributes(attributes...)

	return raw
}

func newAddress(cid *cid.ID, oid *oid.ID) *address.Address {
	addr := address.NewAddress()
	addr.SetContainerID(cid)
	addr.SetObjectID(oid)
	return addr
}

func getNameAttr(obj *object.Object) string {
	for _, attr := range obj.Attributes() {
		if attr.Key() == object.AttributeFileName {
			return attr.Value()
		}
	}

	return obj.ID().String()
}
