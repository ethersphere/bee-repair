package exporter

import (
	"archive/tar"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"github.com/ethersphere/bee/pkg/shed"
	"io"
	"os"
)

const (
	// filename in tar archive that holds the information
	// about exported data format version
	ExportVersionFilename = ".swarm-export-version"
	// current export format version
	CurrentExportVersion = "1"
	// default export filename
	DefaultExportFilename = "swarm-exportdb.tar"
)

type ProgressUpdater interface {
	Update(int, int)
}

type Option func(*exporter)

func WithDestinationFilename(fname string) Option {
	return func(e *exporter) {
		e.dstFile = fname
	}
}

func WithProgressUpdater(upd ProgressUpdater) Option {
	return func(e *exporter) {
		e.updater = upd
	}
}

func Export(src string, opts ...Option) error {
	e, err := newExporter(src, opts...)
	if err != nil {
		return errors.New("Invalid source directory err:" + err.Error())
	}
	return e.export()
}

type noopUpdater struct{}

func (n noopUpdater) Update(_, _ int) {}

type exporter struct {
	retrievalIndex shed.Index
	closer         io.Closer
	dstFile        string
	updater        ProgressUpdater
}

func defaultOpts(e *exporter) {
	if e.dstFile == "" {
		e.dstFile = DefaultExportFilename
	}
	if e.updater == nil {
		e.updater = noopUpdater{}
	}
}

func getRetrievalIndex(src string) (index shed.Index, closer io.Closer, err error) {
	s, e := shed.NewDB(src, nil)
	if e != nil {
		err = e
		return
	}

	index, err = s.NewIndex("Address->StoreTimestamp|BinID|Data", shed.IndexFuncs{
		EncodeKey: func(fields shed.Item) (key []byte, err error) {
			return fields.Address, nil
		},
		DecodeKey: func(key []byte) (e shed.Item, err error) {
			e.Address = key
			return e, nil
		},
		EncodeValue: func(fields shed.Item) (value []byte, err error) {
			b := make([]byte, 16)
			binary.BigEndian.PutUint64(b[:8], fields.BinID)
			binary.BigEndian.PutUint64(b[8:16], uint64(fields.StoreTimestamp))
			value = append(b, fields.Data...)
			return value, nil
		},
		DecodeValue: func(keyItem shed.Item, value []byte) (e shed.Item, err error) {
			e.StoreTimestamp = int64(binary.BigEndian.Uint64(value[8:16]))
			e.BinID = binary.BigEndian.Uint64(value[:8])
			e.Data = value[16:]
			return e, nil
		},
	})

	closer = s
	return
}

func newExporter(src string, opts ...Option) (*exporter, error) {
	e := &exporter{}
	for _, opt := range opts {
		opt(e)
	}
	defaultOpts(e)

	// Index storing actual chunk address, data and bin id.
	idx, closer, err := getRetrievalIndex(src)
	if err != nil {
		return nil, err
	}
	e.retrievalIndex = idx
	e.closer = closer
	return e, nil
}

func (e *exporter) export() error {
	defer e.closer.Close()

	total, err := e.retrievalIndex.Count()
	if err != nil {
		return err
	}

	dstF, err := os.Create(e.dstFile)
	if err != nil {
		return err
	}
	tw := tar.NewWriter(dstF)
	defer tw.Close()

	if err := tw.WriteHeader(&tar.Header{
		Name: ExportVersionFilename,
		Mode: 0644,
		Size: int64(len(CurrentExportVersion)),
	}); err != nil {
		return err
	}
	if _, err := tw.Write([]byte(CurrentExportVersion)); err != nil {
		return err
	}

	doneCount := 0
	e.updater.Update(doneCount, total)

	return e.retrievalIndex.Iterate(func(item shed.Item) (stop bool, err error) {

		hdr := &tar.Header{
			Name: hex.EncodeToString(item.Address),
			Mode: 0644,
			Size: int64(len(item.Data)),
		}

		if err := tw.WriteHeader(hdr); err != nil {
			return false, err
		}
		if _, err := tw.Write(item.Data); err != nil {
			return false, err
		}

		doneCount++
		e.updater.Update(doneCount, total)
		return false, nil
	}, nil)
}
