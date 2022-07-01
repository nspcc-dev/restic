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
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/pool"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

// BuffCloser is wrapper to load files from neofs.
type BuffCloser struct {
	io.Reader
}

func (bc *BuffCloser) Close() error {
	return nil
}

func createPool(ctx context.Context, acc *wallet.Account, cfg Config) (*pool.Pool, error) {
	var prm pool.InitParameters
	prm.SetKey(&acc.PrivateKey().PrivateKey)
	prm.SetNodeDialTimeout(cfg.Timeout)
	prm.SetHealthcheckTimeout(cfg.Timeout)
	prm.SetClientRebalanceInterval(cfg.RebalanceInterval)
	prm.AddNode(pool.NewNodeParam(1, cfg.Endpoint, 1))

	p, err := pool.NewPool(prm)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}

	if err = p.Dial(ctx); err != nil {
		return nil, fmt.Errorf("dial pool: %w", err)
	}

	return p, nil
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

func getContainerID(ctx context.Context, client *pool.Pool, owner user.ID, container string) (cid.ID, error) {
	var cnrID cid.ID
	if err := cnrID.DecodeString(container); err != nil {
		return findContainerID(ctx, client, owner, container)
	}
	return cnrID, nil
}

func findContainerID(ctx context.Context, client *pool.Pool, owner user.ID, containerName string) (cid.ID, error) {
	var prm pool.PrmContainerList
	prm.SetOwnerID(owner)

	containerIDs, err := client.ListContainers(ctx, prm)
	if err != nil {
		return cid.ID{}, fmt.Errorf("list containers: %w", err)
	}

	for _, cnrID := range containerIDs {
		var prmGet pool.PrmContainerGet
		prmGet.SetContainerID(cnrID)

		cnr, err := client.GetContainer(ctx, prmGet)
		if err != nil {
			return cid.ID{}, fmt.Errorf("get container: %w", err)
		}

		for _, attr := range cnr.Attributes() {
			if attr.Key() == container.AttributeName && attr.Value() == containerName {
				return cnrID, nil
			}
		}
	}

	return cid.ID{}, fmt.Errorf("container '%s' not found", containerName)
}

func formRawObject(own *user.ID, cnrID cid.ID, name string, header map[string]string) *object.Object {
	attributes := make([]object.Attribute, 0, 2+len(header))
	filename := object.NewAttribute()
	filename.SetKey(object.AttributeFileName)
	filename.SetValue(name)

	createdAt := object.NewAttribute()
	createdAt.SetKey(object.AttributeTimestamp)
	createdAt.SetValue(strconv.FormatInt(time.Now().UTC().Unix(), 10))

	attributes = append(attributes, *filename, *createdAt)

	for key, val := range header {
		attr := object.NewAttribute()
		attr.SetKey(key)
		attr.SetValue(val)
		attributes = append(attributes, *attr)
	}

	obj := object.New()
	obj.SetOwnerID(own)
	obj.SetContainerID(cnrID)
	obj.SetAttributes(attributes...)

	return obj
}

func newAddress(cnrID cid.ID, objID oid.ID) oid.Address {
	var addr oid.Address
	addr.SetContainer(cnrID)
	addr.SetObject(objID)
	return addr
}

func getNameAttr(obj *object.Object) string {
	for _, attr := range obj.Attributes() {
		if attr.Key() == object.AttributeFileName {
			return attr.Value()
		}
	}

	objID, _ := obj.ID()
	return objID.EncodeToString()
}
