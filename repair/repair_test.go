package repair_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/ethersphere/bee-repair/collection/entry"
	"github.com/ethersphere/bee-repair/repair"
	"github.com/ethersphere/bee/pkg/file/loadsave"
	"github.com/ethersphere/bee/pkg/file/splitter"
	"github.com/ethersphere/bee/pkg/manifest"
	"github.com/ethersphere/bee/pkg/storage"
	"github.com/ethersphere/bee/pkg/storage/mock"
	"github.com/ethersphere/bee/pkg/swarm"
)

type fEntry struct {
	name        string
	filename    string
	dir         string
	contentType string
	size        int64
	reference   swarm.Address
}

func TestFileRepair(t *testing.T) {
	testFiles := []fEntry{
		{
			name:        "file single chunk",
			filename:    "simple.txt",
			contentType: "text/plain; charset=utf-8",
			size:        swarm.ChunkSize,
		},
		{
			name:        "file multiple chunks",
			filename:    "simple.jpeg",
			contentType: "image/jpeg; charset=utf-8",
			size:        swarm.ChunkSize * 5,
		},
		{
			// Filename is bigger than what single node in manifest can hold
			name:        "file large name",
			filename:    "135c88465b7b6da82c134dafc093e6248956d5c003cd8e3566f3d952a0d26180",
			contentType: "image/jpeg; charset=utf-8",
			size:        swarm.ChunkSize / 2,
		},
		{
			name:        "file tar format",
			filename:    "simple.tar",
			contentType: "application/x-tar",
			size:        swarm.ChunkSize * 10,
		},
	}

	for _, f := range testFiles {
		t.Run(f.name, func(t *testing.T) {

			ctx := context.Background()
			store := mock.NewStorer()

			oldReference, err := createFileOldFormat(ctx, store, &f)
			if err != nil {
				t.Fatal(err)
			}

			newReference, err := repair.FileRepair(
				ctx,
				oldReference,
				repair.WithMockStore(store),
			)
			if err != nil {
				t.Fatal(err)
			}

			m, err := manifest.NewDefaultManifestReference(
				newReference,
				loadsave.New(store, storage.ModePutUpload, false),
			)
			if err != nil {
				t.Fatal(err)
			}

			rootEntry, err := m.Lookup(ctx, manifest.RootPath)
			if err != nil {
				t.Fatal(err)
			}
			if rootEntry.Metadata()[manifest.WebsiteIndexDocumentSuffixKey] != f.filename {
				t.Fatal("Invalid manifest root entry")
			}

			fileEntry, err := m.Lookup(ctx, f.filename)
			if err != nil {
				t.Fatal(err)
			}
			if fileEntry.Reference().String() != f.reference.String() {
				t.Fatalf("Invalid manifest file reference, Exp: %s Found: %s",
					fileEntry.Reference(), f.reference)
			}
			if fileEntry.Metadata()[manifest.EntryMetadataFilenameKey] != f.filename {
				t.Fatal("Invalid manifest file metadata: Filename")
			}
			if fileEntry.Metadata()[manifest.EntryMetadataContentTypeKey] != f.contentType {
				t.Fatal("Invalid manifest file metadata: ContentType")
			}
		})
	}
}

type countUpdater struct {
	msgCount int
}

func (s *countUpdater) Update(_ string) {
	s.msgCount++
}

func TestDirectoryRepair(t *testing.T) {
	testDirs := []struct {
		name      string
		indexFile string
		errorFile string
		files     []*fEntry
	}{
		{
			name: "directory simple",
			files: []*fEntry{
				{
					filename:    "simple.txt",
					contentType: "text/plain; charset=utf-8",
					size:        swarm.ChunkSize,
				},
				{
					filename:    "simple.jpeg",
					contentType: "image/jpeg; charset=utf-8",
					size:        swarm.ChunkSize * 5,
				},
			},
		},
		{
			name:      "directory multiple",
			indexFile: "b.jpeg",
			errorFile: "a.txt",
			files: []*fEntry{
				{
					filename:    "a.txt",
					contentType: "text/plain; charset=utf-8",
					size:        swarm.ChunkSize,
				},
				{
					filename:    "b.jpeg",
					contentType: "image/jpeg; charset=utf-8",
					size:        swarm.ChunkSize * 5,
				},
				{
					dir:         "c",
					filename:    "d.tar",
					contentType: "application/x-tar",
					size:        swarm.ChunkSize * 10,
				},
				{
					dir:         "c",
					filename:    "e.jpeg",
					contentType: "image/jpeg; charset=utf-8",
					size:        swarm.ChunkSize * 10,
				},
				{
					dir:         "c/f",
					filename:    "g.txt",
					contentType: "text/plain; charset=utf-8",
					size:        swarm.ChunkSize * 2,
				},
				{
					dir:         "c/f",
					filename:    "h.jpeg",
					contentType: "image/jpeg; charset=utf-8",
					size:        swarm.ChunkSize * 5,
				},
			},
		},
	}

	for _, d := range testDirs {
		t.Run(d.name, func(t *testing.T) {

			ctx := context.Background()
			store := mock.NewStorer()

			oldReference, err := createDirOldFormat(ctx, store, d.indexFile, d.errorFile, d.files)
			if err != nil {
				t.Fatal(err)
			}

			updater := &countUpdater{}

			newReference, err := repair.DirectoryRepair(
				ctx,
				oldReference,
				repair.WithMockStore(store),
				repair.WithProgressUpdater(updater),
			)
			if err != nil {
				t.Fatal(err)
			}

			// We get 1 update msg per file
			if updater.msgCount != len(d.files) {
				t.Fatal("Progress updater update mismatch")
			}

			m, err := manifest.NewDefaultManifestReference(
				newReference,
				loadsave.New(store, storage.ModePutUpload, false),
			)
			if err != nil {
				t.Fatal(err)
			}

			if d.indexFile != "" || d.errorFile != "" {
				rootEntry, err := m.Lookup(ctx, manifest.RootPath)
				if err != nil {
					t.Fatal(err)
				}
				if d.indexFile != "" {
					if rootEntry.Metadata()[manifest.WebsiteIndexDocumentSuffixKey] != d.indexFile {
						t.Fatal("Invalid manifest root entry")
					}
				}
				if d.errorFile != "" {
					if rootEntry.Metadata()[manifest.WebsiteErrorDocumentPathKey] != d.errorFile {
						t.Fatal("Invalid manifest root entry")
					}
				}
			}
			for _, v := range d.files {
				fileEntry, err := m.Lookup(ctx, filepath.Join(v.dir, v.filename))
				if err != nil {
					t.Fatal(err)
				}
				if fileEntry.Reference().String() != v.reference.String() {
					t.Fatalf("Invalid manifest file reference, Exp: %s Found: %s",
						v.reference, fileEntry.Reference())
				}
				if fileEntry.Metadata()[manifest.EntryMetadataFilenameKey] != v.filename {
					t.Fatal("Invalid manifest file metadata: Filename")
				}
				if fileEntry.Metadata()[manifest.EntryMetadataContentTypeKey] != v.contentType {
					t.Fatal("Invalid manifest file metadata: ContentType")
				}
			}
		})
	}
}

// putEntry creates a new file entry with the given reference.
func createFileOldFormat(ctx context.Context, store storage.Storer, f *fEntry) (swarm.Address, error) {
	// set up splitter to process the metadata
	s := splitter.NewSimpleSplitter(store, storage.ModePutUpload)

	fdata := make([]byte, f.size)
	_, err := rand.Read(fdata)
	if err != nil {
		return swarm.ZeroAddress, err
	}
	fileBuf := bytes.NewBuffer(fdata)
	fileBytesReader := io.LimitReader(fileBuf, int64(len(fdata)))
	fileBytesReadCloser := ioutil.NopCloser(fileBytesReader)
	fileBytesAddr, err := s.Split(ctx, fileBytesReadCloser, int64(len(fdata)), false)
	if err != nil {
		return swarm.ZeroAddress, err
	}

	metadata := entry.NewMetadata(f.filename)
	metadata.MimeType = f.contentType

	// serialize metadata and send it to splitter
	metadataBytes, err := json.Marshal(metadata)
	if err != nil {
		return swarm.ZeroAddress, err
	}
	// logger.Debugf("metadata contents: %s", metadataBytes)

	// first add metadata
	metadataBuf := bytes.NewBuffer(metadataBytes)
	metadataReader := io.LimitReader(metadataBuf, int64(len(metadataBytes)))
	metadataReadCloser := ioutil.NopCloser(metadataReader)
	metadataAddr, err := s.Split(ctx, metadataReadCloser, int64(len(metadataBytes)), false)
	if err != nil {
		return swarm.ZeroAddress, err
	}

	// create entry from given reference and metadata,
	// serialize and send to splitter
	fileEntry := entry.New(fileBytesAddr, metadataAddr)
	fileEntryBytes, err := fileEntry.MarshalBinary()
	if err != nil {
		return swarm.ZeroAddress, err
	}
	fileEntryBuf := bytes.NewBuffer(fileEntryBytes)
	fileEntryReader := io.LimitReader(fileEntryBuf, int64(len(fileEntryBytes)))
	fileEntryReadCloser := ioutil.NopCloser(fileEntryReader)
	fileEntryAddr, err := s.Split(ctx, fileEntryReadCloser, int64(len(fileEntryBytes)), false)
	if err != nil {
		return swarm.ZeroAddress, err
	}

	f.reference = fileBytesAddr
	return fileEntryAddr, nil
}

func createDirOldFormat(
	ctx context.Context,
	store storage.Storer,
	indexFile,
	errorFile string,
	files []*fEntry,
) (swarm.Address, error) {
	m, err := manifest.NewDefaultManifest(
		loadsave.New(store, storage.ModePutUpload, false),
		false,
	)
	if err != nil {
		return swarm.ZeroAddress, err
	}

	var rootMtdt map[string]string

	if indexFile != "" || errorFile != "" {
		rootMtdt = make(map[string]string)
		if indexFile != "" {
			rootMtdt[manifest.WebsiteIndexDocumentSuffixKey] = indexFile
		}
		if errorFile != "" {
			rootMtdt[manifest.WebsiteErrorDocumentPathKey] = errorFile
		}
	}

	err = m.Add(ctx, manifest.RootPath, manifest.NewEntry(swarm.ZeroAddress, rootMtdt))
	if err != nil {
		return swarm.ZeroAddress, err
	}

	for _, f := range files {
		fileRef, err := createFileOldFormat(ctx, store, f)
		if err != nil {
			return swarm.ZeroAddress, err
		}
		err = m.Add(ctx, filepath.Join(f.dir, f.filename), manifest.NewEntry(fileRef, nil))
		if err != nil {
			return swarm.ZeroAddress, err
		}
	}
	newManifest, err := m.Store(ctx)
	if err != nil {
		return swarm.ZeroAddress, err
	}
	return newManifest, nil
}
