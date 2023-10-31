package flatfs

import (
	"context"
	"fmt"
	"github.com/4everland/ipfs-top/third_party/dag"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/datastore/dshelp"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	"path/filepath"

	"github.com/ipfs/kubo/plugin"
	"github.com/ipfs/kubo/repo"
	"github.com/ipfs/kubo/repo/fsrepo"

	flatfs "github.com/ipfs/go-ds-flatfs"
)

// Plugins is exported list of plugins that will be loaded.
var Plugins = []plugin.Plugin{
	&flatfsPlugin{},
}

type flatfsPlugin struct{}

var _ plugin.PluginDatastore = (*flatfsPlugin)(nil)

func (*flatfsPlugin) Name() string {
	return "ds-flatfs"
}

func (*flatfsPlugin) Version() string {
	return "0.1.0"
}

func (*flatfsPlugin) Init(_ *plugin.Environment) error {
	return nil
}

func (*flatfsPlugin) DatastoreTypeName() string {
	return "flatfs"
}

type datastoreConfig struct {
	path      string
	shardFun  *flatfs.ShardIdV1
	syncField bool
	bs        blockstore.Blockstore
}

// BadgerdsDatastoreConfig returns a configuration stub for a badger datastore
// from the given parameters.
func (*flatfsPlugin) DatastoreConfigParser() fsrepo.ConfigFromMap {
	return func(params map[string]interface{}) (fsrepo.DatastoreConfig, error) {
		var c datastoreConfig
		var ok bool
		var err error

		c.path, ok = params["path"].(string)
		if !ok {
			return nil, fmt.Errorf("'path' field is missing or not boolean")
		}

		sshardFun, ok := params["shardFunc"].(string)
		if !ok {
			return nil, fmt.Errorf("'shardFunc' field is missing or not a string")
		}
		c.shardFun, err = flatfs.ParseShardFunc(sshardFun)
		if err != nil {
			return nil, err
		}

		c.syncField, ok = params["sync"].(bool)
		if !ok {
			return nil, fmt.Errorf("'sync' field is missing or not boolean")
		}

		endpoint, ok := params["endpoint"].(string)
		if !ok {
			return nil, fmt.Errorf("'endpoint' field is missing or not string")
		}

		c.bs, err = dag.NewBlockStore(endpoint, "")
		if err != nil {
			return nil, fmt.Errorf("blockstore init err: %v", err)
		}

		return &c, nil
	}
}

func (c *datastoreConfig) DiskSpec() fsrepo.DiskSpec {
	return map[string]interface{}{
		"type":      "flatfs",
		"path":      c.path,
		"shardFunc": c.shardFun.String(),
	}
}

func (c *datastoreConfig) Create(path string) (repo.Datastore, error) {
	p := c.path
	if !filepath.IsAbs(p) {
		p = filepath.Join(path, p)
	}
	fs, err := flatfs.CreateOrOpen(p, c.shardFun, c.syncField)
	if err != nil {
		return nil, err
	}

	return &rpcDs{
		bs:        c.bs,
		Datastore: fs,
	}, nil
}

type rpcDs struct {
	bs blockstore.Blockstore
	*flatfs.Datastore
}

func (d *rpcDs) Get(ctx context.Context, key ds.Key) (value []byte, err error) {
	if value, err = d.get(ctx, key, cid.Raw); err == nil {
		return
	}

	if value, err = d.get(ctx, key, cid.DagProtobuf); err == nil {
		return
	}

	return d.Datastore.Get(ctx, key)
}

func (d *rpcDs) Has(ctx context.Context, key ds.Key) (exists bool, err error) {
	if exists, _ = d.has(ctx, key, cid.Raw); exists {
		return
	}

	if exists, _ = d.has(ctx, key, cid.DagProtobuf); exists {
		return
	}

	return d.Datastore.Has(ctx, key)
}

func (d *rpcDs) GetSize(ctx context.Context, key ds.Key) (size int, err error) {
	if size, err = d.getSize(ctx, key, cid.Raw); err == nil {
		return
	}

	if size, err = d.getSize(ctx, key, cid.DagProtobuf); err == nil {
		return
	}

	return d.Datastore.GetSize(ctx, key)
}

func (d *rpcDs) get(ctx context.Context, key ds.Key, codecType uint64) ([]byte, error) {
	c, err := dshelp.DsKeyToCidV1(key, codecType)
	if err != nil {
		return nil, err
	}
	b, err := d.bs.Get(ctx, c)
	if err != nil {
		return nil, err
	}
	return b.RawData(), nil
}

func (d *rpcDs) has(ctx context.Context, key ds.Key, codecType uint64) (bool, error) {
	c, err := dshelp.DsKeyToCidV1(key, codecType)
	if err != nil {
		return false, err
	}
	return d.bs.Has(ctx, c)
}

func (d *rpcDs) getSize(ctx context.Context, key ds.Key, codecType uint64) (int, error) {
	c, err := dshelp.DsKeyToCidV1(key, codecType)
	if err != nil {
		return 0, err
	}
	return d.bs.GetSize(ctx, c)
}
