package neofs

import (
	"context"
	"io"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"github.com/nspcc-dev/neofs-sdk-go/acl"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/policy"
	"github.com/nspcc-dev/neofs-sdk-go/pool"
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
	versions := []string{"0.24.0", "0.25.1", "0.26.0", "0.27.0", "latest"}

	cfg := Config{
		Endpoint:          "localhost:8080",
		Container:         "container",
		Wallet:            filename,
		Timeout:           10 * time.Second,
		RebalanceInterval: 20 * time.Second,
	}

	for _, version := range versions {
		ctx, cancel := context.WithCancel(rootCtx)
		aioContainer := createDockerContainer(ctx, t, aioImage+version)

		p, err := createPool(ctx, cfg)
		require.NoError(t, err)
		_, err = createContainer(ctx, p, cfg.Container, "REP 1")
		require.NoError(t, err)

		backend, err := Open(ctx, cfg)
		require.NoError(t, err)

		t.Run("simple store and load "+version, func(t *testing.T) { simpleStoreLoad(ctx, t, backend) })

		err = aioContainer.Terminate(ctx)
		require.NoError(t, err)
		cancel()
	}
}

func simpleStoreLoad(ctx context.Context, t *testing.T, backend restic.Backend) {
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

func waitPresence(ctx context.Context, cli pool.Container, cnrID *cid.ID) error {
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
