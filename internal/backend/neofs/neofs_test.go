//go:build go1.17 && linux
// +build go1.17,linux

package neofs

import (
	"context"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	"github.com/nspcc-dev/neofs-sdk-go/container/acl"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/pool"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/restic/restic/internal/restic"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestIntegration(t *testing.T) {
	filename := createWallet(t)
	defer os.Remove(filename)

	rootCtx := context.Background()
	aioImage := "nspccdev/neofs-aio-testcontainer:"
	versions := []string{
		"0.27.7",
		"0.28.1",
		"0.29.0",
		"latest",
	}

	cfg := Config{
		Endpoints:         "localhost:8080",
		RPCEndpoint:       "http://localhost:30333",
		Container:         "container",
		Wallet:            filename,
		Timeout:           10 * time.Second,
		RebalanceInterval: 20 * time.Second,
		Connections:       1,
	}

	acc, err := getAccount(cfg)
	require.NoError(t, err)

	var owner user.ID
	user.IDFromKey(&owner, acc.PrivateKey().PrivateKey.PublicKey)

	for _, version := range versions {
		ctx, cancel := context.WithCancel(rootCtx)
		aioContainer := createDockerContainer(ctx, t, aioImage+version)

		p, err := createPool(ctx, acc, cfg)
		require.NoError(t, err)
		_, err = createContainer(ctx, p, owner, cfg.Container, "REP 1")
		require.NoError(t, err)

		backend, err := Open(ctx, cfg)
		require.NoError(t, err)

		t.Run("simple store load delete "+version, func(t *testing.T) { simpleStoreLoadDelete(ctx, t, backend) })
		t.Run("list "+version, func(t *testing.T) { simpleList(ctx, t, backend) })

		err = aioContainer.Terminate(ctx)
		require.NoError(t, err)
		cancel()
	}
}

func simpleStoreLoadDelete(ctx context.Context, t *testing.T, backend restic.Backend) {
	h := restic.Handle{
		Type:              restic.PackFile,
		ContainedBlobType: restic.DataBlob,
		Name:              "some-file",
	}

	content := []byte("content")

	err := backend.Save(ctx, h, restic.NewByteReader(content, nil))
	require.NoError(t, err)

	err = backend.Load(ctx, h, 0, 0, func(rd io.Reader) error {
		data, err := io.ReadAll(rd)
		require.NoError(t, err)
		require.Equal(t, content, data)
		return nil
	})
	require.NoError(t, err)

	err = backend.Remove(ctx, h)
	require.NoError(t, err)
}

func simpleList(ctx context.Context, t *testing.T, backend restic.Backend) {
	h := restic.Handle{
		Type:              restic.PackFile,
		ContainedBlobType: restic.DataBlob,
		Name:              "some-file-for-list",
	}

	content := []byte("content")

	err := backend.Save(ctx, h, restic.NewByteReader(content, nil))
	require.NoError(t, err)

	var count int
	err = backend.List(ctx, restic.PackFile, func(info restic.FileInfo) error {
		count++
		require.Equal(t, h.Name, info.Name)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

func createContainer(ctx context.Context, client *pool.Pool, owner user.ID, containerName, placementPolicy string) (cid.ID, error) {
	var pp netmap.PlacementPolicy
	if err := pp.DecodeString(placementPolicy); err != nil {
		return cid.ID{}, fmt.Errorf("decode policy: %w", err)
	}

	var cnr container.Container
	cnr.Init()
	cnr.SetPlacementPolicy(pp)
	cnr.SetBasicACL(acl.Private)
	cnr.SetOwner(owner)

	container.SetName(&cnr, containerName)
	container.SetCreationTime(&cnr, time.Now())

	var domain container.Domain
	domain.SetName(containerName)
	container.WriteDomain(&cnr, domain)

	var wp pool.WaitParams
	wp.SetPollInterval(5 * time.Second)
	wp.SetTimeout(30 * time.Second)
	var prm pool.PrmContainerPut
	prm.SetContainer(cnr)
	prm.SetWaitParams(wp)

	containerID, err := client.PutContainer(ctx, prm)
	if err != nil {
		return cid.ID{}, fmt.Errorf("put container: %w", err)
	}

	return containerID, nil
}

func createWallet(t *testing.T) string {
	file, err := os.CreateTemp("", "wallet")
	require.NoError(t, err)
	err = file.Close()
	require.NoError(t, err)

	key, err := keys.NewPrivateKeyFromHex("1dd37fba80fec4e6a6f13fd708d8dcb3b29def768017052f6c930fa1c5d90bbb")
	require.NoError(t, err)

	w, err := wallet.NewWallet(file.Name())
	require.NoError(t, err)

	acc := wallet.NewAccountFromPrivateKey(key)
	err = acc.Encrypt("", w.Scrypt)
	require.NoError(t, err)

	w.AddAccount(acc)
	err = w.Save()
	require.NoError(t, err)
	w.Close()

	return file.Name()
}

func createDockerContainer(ctx context.Context, t *testing.T, image string) testcontainers.Container {
	req := testcontainers.ContainerRequest{
		Image:       image,
		WaitingFor:  wait.NewLogStrategy("aio container started").WithStartupTimeout(30 * time.Second),
		Name:        "aio",
		Hostname:    "aio",
		NetworkMode: "host",
	}
	aioC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)

	return aioC
}
