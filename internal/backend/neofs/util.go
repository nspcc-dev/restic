package neofs

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/nspcc-dev/neo-go/cli/flags"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/ns"
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

// EndpointInfo stores information about endpoint.
type EndpointInfo struct {
	Address  string
	Priority int
	Weight   float64
}

func parseEndpoints(endpointsParam string) ([]EndpointInfo, error) {
	var err error
	expectedLength := -1 // to make sure all endpoints have the same format

	endpoints := splitAndTrimEmpty(endpointsParam, ";")
	res := make([]EndpointInfo, 0, len(endpoints))
	seen := make(map[string]struct{}, len(endpoints))

	for _, endpoint := range endpoints {
		endpointInfoSplit := splitAndTrimEmpty(endpoint, " ")
		address := endpointInfoSplit[0]

		if len(address) == 0 {
			continue
		}
		if _, ok := seen[address]; ok {
			return nil, fmt.Errorf("endpoint '%s' is already defined", address)
		}
		seen[address] = struct{}{}

		endpointInfo := EndpointInfo{
			Address:  address,
			Priority: 1,
			Weight:   1,
		}

		if expectedLength == -1 {
			expectedLength = len(endpointInfoSplit)
		}

		if len(endpointInfoSplit) != expectedLength {
			return nil, fmt.Errorf("all endpoints must have the same format: '%s'", endpointsParam)
		}

		switch len(endpointInfoSplit) {
		case 1:
		case 2:
			endpointInfo.Priority, err = parsePriority(endpointInfoSplit[1])
			if err != nil {
				return nil, fmt.Errorf("invalid endpoint '%s': %w", endpoint, err)
			}
		case 3:
			endpointInfo.Priority, err = parsePriority(endpointInfoSplit[1])
			if err != nil {
				return nil, fmt.Errorf("invalid endpoint '%s': %w", endpoint, err)
			}

			endpointInfo.Weight, err = parseWeight(endpointInfoSplit[2])
			if err != nil {
				return nil, fmt.Errorf("invalid endpoint '%s': %w", endpoint, err)
			}
		default:
			return nil, fmt.Errorf("invalid endpoint format '%s'", endpoint)
		}

		res = append(res, endpointInfo)
	}

	return res, nil
}

func parsePriority(priorityStr string) (int, error) {
	priority, err := strconv.Atoi(priorityStr)
	if err != nil {
		return 0, fmt.Errorf("invalid priority '%s': %w", priorityStr, err)
	}
	if priority <= 0 {
		return 0, fmt.Errorf("priority must be positive '%s'", priorityStr)
	}

	return priority, nil
}

func parseWeight(weightStr string) (float64, error) {
	weight, err := strconv.ParseFloat(weightStr, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid weight '%s': %w", weightStr, err)
	}
	if weight <= 0 {
		return 0, fmt.Errorf("weight must be positive '%s'", weightStr)
	}

	return weight, nil
}

func splitAndTrimEmpty(str, sep string) []string {
	var res []string
	for _, item := range strings.Split(strings.TrimSpace(str), sep) {
		trimmed := strings.TrimSpace(item)
		if len(trimmed) > 0 {
			res = append(res, trimmed)
		}
	}

	return res
}

func createPool(ctx context.Context, acc *wallet.Account, cfg Config) (*pool.Pool, error) {
	var prm pool.InitParameters
	prm.SetKey(&acc.PrivateKey().PrivateKey)
	prm.SetNodeDialTimeout(cfg.Timeout)
	prm.SetHealthcheckTimeout(cfg.Timeout)
	prm.SetClientRebalanceInterval(cfg.RebalanceInterval)

	nodes, err := getNodePoolParams(cfg.Endpoints)
	if err != nil {
		return nil, err
	}
	for _, node := range nodes {
		prm.AddNode(node)
	}

	p, err := pool.NewPool(prm)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}

	if err = p.Dial(ctx); err != nil {
		return nil, fmt.Errorf("dial pool: %w", err)
	}

	return p, nil
}

func getNodePoolParams(endpointParam string) ([]pool.NodeParam, error) {
	endpointInfos, err := parseEndpoints(endpointParam)
	if err != nil {
		return nil, fmt.Errorf("parse endpoints params: %w", err)
	}

	res := make([]pool.NodeParam, len(endpointInfos))
	for i, info := range endpointInfos {
		res[i] = pool.NewNodeParam(info.Priority, info.Address, info.Weight)
	}

	return res, nil
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

func resolveContainerID(rpcAddress string, container string) (cid.ID, error) {
	var cnrID cid.ID
	if err := cnrID.DecodeString(container); err != nil {
		var nnsResolver ns.NNS
		if err = nnsResolver.Dial(rpcAddress); err != nil {
			return cid.ID{}, fmt.Errorf("failed to dial rpc endpoint '%s': %w", rpcAddress, err)
		}

		return nnsResolver.ResolveContainerName(container)
	}
	return cnrID, nil
}

func formRawObject(own *user.ID, cnrID cid.ID, name string, header map[string]string) *object.Object {
	attributes := make([]object.Attribute, 0, 2+len(header))
	filename := object.NewAttribute()
	filename.SetKey(object.AttributeFilePath)
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

func getFilePathAttr(obj object.Object) string {
	for _, attr := range obj.Attributes() {
		if attr.Key() == object.AttributeFilePath {
			return attr.Value()
		}
	}

	objID, _ := obj.ID()
	return objID.EncodeToString()
}
