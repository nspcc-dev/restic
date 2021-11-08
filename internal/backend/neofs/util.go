package neofs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/nspcc-dev/neo-go/cli/flags"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"github.com/nspcc-dev/neofs-api-go/pkg/acl"
	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	cid "github.com/nspcc-dev/neofs-api-go/pkg/container/id"
	"github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	"github.com/nspcc-dev/neofs-sdk-go/policy"
	"github.com/nspcc-dev/neofs-sdk-go/pool"
)

// BuffCloser is wrapper to load files from neofs.
type BuffCloser struct {
	io.Reader
}

func (bc *BuffCloser) Close() error {
	return nil
}

var ErrNotFound = errors.New("not found")

func createPool(ctx context.Context, cfg Config) (pool.Pool, error) {
	acc, err := getAccount(cfg)
	if err != nil {
		return nil, err
	}

	pb := new(pool.Builder)
	pb.AddNode(cfg.Endpoint, 1)

	opts := &pool.BuilderOptions{
		Key:                     &acc.PrivateKey().PrivateKey,
		NodeConnectionTimeout:   cfg.Timeout,
		NodeRequestTimeout:      cfg.Timeout,
		ClientRebalanceInterval: cfg.RebalanceInterval,
		SessionExpirationEpoch:  cfg.SessionExpiration,
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

func getContainerID(ctx context.Context, client pool.Pool, containerName, placementPolicy string) (*cid.ID, error) {
	cnrID, err := findContainerID(ctx, client, containerName)
	if err != nil && errors.Is(err, ErrNotFound) {
		return createContainer(ctx, client, containerName, placementPolicy)
	}

	return cnrID, err
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

	return nil, ErrNotFound
}

func createContainer(ctx context.Context, client pool.Pool, containerName, placementPolicy string) (*cid.ID, error) {
	pp, err := policy.Parse(placementPolicy)
	if err != nil {
		return nil, err
	}

	cnr := container.New(
		container.WithPolicy((*netmap.PlacementPolicy)(pp)),
		container.WithCustomBasicACL(acl.PrivateBasicRule),
		container.WithAttribute(container.AttributeName, containerName),
		container.WithAttribute(container.AttributeTimestamp, strconv.FormatInt(time.Now().Unix(), 10)))
	cnr.SetOwnerID(client.OwnerID())

	containerID, err := client.PutContainer(ctx, cnr)
	if err != nil {
		return nil, err
	}

	err = waitPresence(ctx, client, containerID)
	return containerID, err
}

func waitPresence(ctx context.Context, cli client.Container, cnrID *cid.ID) error {
	wctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	ticker := time.NewTimer(5 * time.Second)
	defer ticker.Stop()
	wdone := wctx.Done()
	done := ctx.Done()
	for {
		select {
		case <-done:
			return ctx.Err()
		case <-wdone:
			return wctx.Err()
		case <-ticker.C:
			_, err := cli.GetContainer(ctx, cnrID)
			if err == nil {
				return nil
			}
			ticker.Reset(5 * time.Second)
		}
	}
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

func newAddress(cid *cid.ID, oid *object.ID) *object.Address {
	address := object.NewAddress()
	address.SetContainerID(cid)
	address.SetObjectID(oid)
	return address
}

func getNameAttr(obj *object.Object) string {
	for _, attr := range obj.Attributes() {
		if attr.Key() == object.AttributeFileName {
			return attr.Value()
		}
	}

	return obj.ID().String()
}
