package neofs

import (
	"context"
	"io"
	"math"
	"os"
	"testing"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/policy"
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
	versions := []string{"0.24.0", "0.25.1", "latest"}

	cfg := Config{
		Endpoint:          "localhost:8080",
		Container:         "container",
		Wallet:            filename,
		Timeout:           10 * time.Second,
		RebalanceInterval: 20 * time.Second,
		SessionExpiration: math.MaxUint32,
		Policy:            "REP 1",
	}

	for _, version := range versions {
		ctx, cancel := context.WithCancel(rootCtx)
		aioContainer := createDockerContainer(ctx, t, aioImage+version)

		backend, err := Open(ctx, cfg)
		require.NoError(t, err)

		t.Run("simple store and load "+version, func(t *testing.T) { simpleStoreLoad(ctx, t, backend) })

		err = aioContainer.Terminate(ctx)
		require.NoError(t, err)
		cancel()
	}
}

func TestPolicy(t *testing.T) {
	pp, err := policy.Parse("REP 3")
	require.NoError(t, err)

	placementPolicy := (*netmap.PlacementPolicy)(pp)
	require.NotNil(t, placementPolicy)
	require.Equal(t, placementPolicy.Replicas()[0].Count(), uint32(3))
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
