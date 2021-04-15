package repair

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/ethersphere/bee-repair/collection/entry"
	cmdfile "github.com/ethersphere/bee-repair/file"
	"github.com/ethersphere/bee/pkg/file"
	"github.com/ethersphere/bee/pkg/file/joiner"
	"github.com/ethersphere/bee/pkg/file/loadsave"
	"github.com/ethersphere/bee/pkg/logging"
	"github.com/ethersphere/bee/pkg/manifest"
	"github.com/ethersphere/bee/pkg/manifest/mantaray"
	"github.com/ethersphere/bee/pkg/storage"
	"github.com/ethersphere/bee/pkg/swarm"
	"io/ioutil"
)

const (
	limitMetadataLength = swarm.ChunkSize
)

type ProgressUpdater interface {
	Update(string)
}

type Option func(*Repairer)

func WithApiStore(host string, port int, useSSL bool) Option {
	return func(c *Repairer) {
		c.store = cmdfile.NewApiStore(host, port, useSSL)
	}
}

func WithLogger(l logging.Logger) Option {
	return func(c *Repairer) {
		c.logger = l
	}
}

func WithEncryption(val bool) Option {
	return func(c *Repairer) {
		c.encrypt = val
	}
}

func WithProgressUpdater(upd ProgressUpdater) Option {
	return func(c *Repairer) {
		c.updater = upd
	}
}

func FileRepair(ctx context.Context, addr swarm.Address, opts ...Option) (swarm.Address, error) {
	r := newWithOptions(opts...)

	oldEntry, err := r.getOldFileEntry(ctx, addr)
	if err != nil {
		return swarm.ZeroAddress, err
	}

	r.updater.Update(fmt.Sprintf("Updating reference for file %s", oldEntry.mtdt.Filename))

	newManifest, err := manifest.NewDefaultManifest(r.ls, false)
	if err != nil {
		return swarm.ZeroAddress, err
	}

	err = newManifest.Add(ctx, manifest.RootPath, manifest.NewEntry(
		swarm.ZeroAddress,
		map[string]string{
			manifest.WebsiteIndexDocumentSuffixKey: oldEntry.mtdt.Filename,
		},
	))
	if err != nil {
		return swarm.ZeroAddress, err
	}

	err = newManifest.Add(
		ctx,
		oldEntry.mtdt.Filename,
		manifest.NewEntry(oldEntry.e.Reference(), map[string]string{
			manifest.EntryMetadataFilenameKey:    oldEntry.mtdt.Filename,
			manifest.EntryMetadataContentTypeKey: oldEntry.mtdt.MimeType,
		}),
	)
	if err != nil {
		return swarm.ZeroAddress, err
	}

	newReference, err := newManifest.Store(ctx)
	if err != nil {
		return swarm.ZeroAddress, err
	}

	r.logger.Debugf("Created new file manifest with reference %s", newReference.String())

	return newReference, nil
}

func DirectoryRepair(ctx context.Context, addr swarm.Address, opts ...Option) (swarm.Address, error) {
	r := newWithOptions(opts...)

	dir, err := r.getOldDirectoryEntry(ctx, addr)
	if err != nil {
		return swarm.ZeroAddress, err
	}

loop:
	for {
		select {
		case f, ok := <-dir.filesC:
			if !ok {
				break loop
			}
			r.updater.Update(fmt.Sprintf("Updating reference for file %s", f.mtdt.Filename))
			err := dir.m.Add(
				ctx,
				f.filepath,
				manifest.NewEntry(f.e.Reference(), map[string]string{
					manifest.EntryMetadataFilenameKey:    f.mtdt.Filename,
					manifest.EntryMetadataContentTypeKey: f.mtdt.MimeType,
				}),
			)
			if err != nil {
				return swarm.ZeroAddress, err
			}
		case e, ok := <-dir.errC:
			if !ok {
				break loop
			}
			return swarm.ZeroAddress, e
		case <-ctx.Done():
			if ctx.Err() != nil {
				return swarm.ZeroAddress, ctx.Err()
			}
			break loop
		}
	}

	newReference, err := dir.m.Store(ctx)
	if err != nil {
		return swarm.ZeroAddress, err
	}

	r.logger.Debugf("Created new directory manifest with reference %s", newReference.String())

	return newReference, nil
}

type Repairer struct {
	store   cmdfile.PutGetter
	ls      file.LoadSaver
	logger  logging.Logger
	encrypt bool
	updater ProgressUpdater
}

type noopUpdater struct{}

func (n *noopUpdater) Update(_ string) {}

func defaultOpts(c *Repairer) {
	if c.store == nil {
		c.store = cmdfile.NewApiStore("127.0.0.1", 1633, false)
	}
	if c.updater == nil {
		c.updater = &noopUpdater{}
	}
	if c.logger == nil {
		c.logger = logging.New(ioutil.Discard, 0)
	}
}

func newWithOptions(opts ...Option) *Repairer {
	r := &Repairer{}
	for _, opt := range opts {
		opt(r)
	}
	defaultOpts(r)
	r.ls = loadsave.New(r.store, storage.ModePutUpload, r.encrypt)
	return r
}

type fileEntry struct {
	filepath string
	e        *entry.Entry
	mtdt     *entry.Metadata
}

type dirEntry struct {
	m      manifest.Interface
	filesC <-chan *fileEntry
	errC   <-chan error
}

func (r *Repairer) getOldFileEntry(ctx context.Context, addr swarm.Address) (*fileEntry, error) {
	buf := bytes.NewBuffer(nil)
	writeCloser := cmdfile.NopWriteCloser(buf)
	limitBuf := cmdfile.NewLimitWriteCloser(writeCloser, limitMetadataLength)

	j, _, err := joiner.New(ctx, r.store, addr)
	if err != nil {
		return nil, err
	}

	_, err = file.JoinReadAll(ctx, j, limitBuf)
	if err != nil {
		return nil, err
	}
	e := &entry.Entry{}
	err = e.UnmarshalBinary(buf.Bytes())
	if err != nil {
		return nil, err
	}

	j, _, err = joiner.New(ctx, r.store, e.Metadata())
	if err != nil {
		return nil, err
	}

	buf = bytes.NewBuffer(nil)

	_, err = file.JoinReadAll(ctx, j, buf)
	if err != nil {
		return nil, err
	}

	// retrieve metadata
	metaData := &entry.Metadata{}
	err = json.Unmarshal(buf.Bytes(), metaData)
	if err != nil {
		return nil, err
	}
	r.logger.Debugf("Read old file entry Filename: %s MIME-type: %s Reference: %s",
		e.Reference(), metaData.Filename, metaData.MimeType)

	return &fileEntry{
		e:    e,
		mtdt: metaData,
	}, nil
}

func (r *Repairer) getOldDirectoryEntry(ctx context.Context, addr swarm.Address) (*dirEntry, error) {
	j, _, err := joiner.New(ctx, r.store, addr)
	if err != nil {
		return nil, err
	}

	buf := bytes.NewBuffer(nil)

	_, err = file.JoinReadAll(ctx, j, buf)
	if err != nil {
		return nil, err
	}

	node := new(mantaray.Node)
	err = node.UnmarshalBinary(buf.Bytes())
	if err != nil {
		return nil, err
	}

	entryChan := make(chan *fileEntry)
	walkFn := func(path []byte, isDir bool, err error) error {
		if err != nil {
			return err
		}
		if !isDir {
			fnode, err := node.LookupNode(ctx, path, r.ls)
			if err != nil {
				return err
			}
			fentry, err := r.getOldFileEntry(ctx, swarm.NewAddress(fnode.Entry()))
			if err != nil {
				return err
			}
			fentry.filepath = string(path)
			entryChan <- fentry
		}
		return nil
	}

	rootNode, err := node.LookupNode(ctx, []byte(manifest.RootPath), r.ls)
	if err != nil {
		return nil, err
	}

	errChan := make(chan error)
	go func() {
		defer close(entryChan)
		defer close(errChan)
		err = node.Walk(ctx, []byte{}, r.ls, walkFn)
		if err != nil {
			errChan <- err
		}
	}()

	m, err := manifest.NewDefaultManifest(r.ls, r.encrypt)
	if err != nil {
		return nil, err
	}

	err = m.Add(ctx, manifest.RootPath, manifest.NewEntry(swarm.ZeroAddress, rootNode.Metadata()))
	if err != nil {
		return nil, err
	}

	r.logger.Debugf("Walking directory %s root metadata: %v", addr.String(), rootNode.Metadata())

	return &dirEntry{
		m:      m,
		filesC: entryChan,
		errC:   errChan,
	}, nil
}
