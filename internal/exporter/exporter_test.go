package exporter_test

import (
	"archive/tar"
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ethersphere/bee-repair/internal/exporter"
	"github.com/ethersphere/bee/pkg/shed"
	chunktesting "github.com/ethersphere/bee/pkg/storage/testing"
	"github.com/ethersphere/bee/pkg/swarm"
)

type checkUpdater struct {
	prev int
	t    *testing.T
}

func (c *checkUpdater) Update(done, total int) {
	if c.prev > done {
		c.t.Fatal("update arrive with older progress")
	}
	if done > total {
		c.t.Fatal("incorrect update")
	}
	c.prev = done
}

func TestExporter(t *testing.T) {
	verifyTar := func(t *testing.T, tr *tar.Reader, chunkMap map[string]swarm.Chunk) {
		for {
			hdr, err := tr.Next()
			if err != nil {
				if err == io.EOF {
					break
				}
				t.Fatal(err)
			}
			if hdr.Name == exporter.ExportVersionFilename {
				if hdr.Size != int64(len(exporter.CurrentExportVersion)) {
					t.Fatal("invalid header size for version entry")
				}
				continue
			}

			chunk, found := chunkMap[hdr.Name]
			if !found {
				t.Fatalf("chunk %s not found", hdr.Name)
			}

			if hdr.Size != int64(len(chunk.Data())) {
				t.Fatalf("invalid chunksize, expected %d got %d", len(chunk.Data()), hdr.Size)
			}

			limitRd := &io.LimitedReader{
				R: tr,
				N: hdr.Size,
			}
			chunkBuf := make([]byte, hdr.Size)
			_, err = limitRd.Read(chunkBuf)
			if err != nil && err != io.EOF {
				t.Fatal(err)
			}
			if !bytes.Equal(chunk.Data(), chunkBuf) {
				t.Fatal("invalid data bytes")
			}
		}
	}

	t.Run("default", func(t *testing.T) {
		defer os.RemoveAll("src")
		defer os.RemoveAll(filepath.Join(".", exporter.DefaultExportFilename))

		err := os.Mkdir("src", 0775)
		if err != nil {
			t.Fatal(err)
		}

		chMap, err := createTestStore("src")
		if err != nil {
			t.Fatal(err)
		}

		err = exporter.Export("src")
		if err != nil {
			t.Fatal(err)
		}

		tarFile, err := os.Open(exporter.DefaultExportFilename)
		if err != nil {
			t.Fatal(err)
		}
		tr := tar.NewReader(tarFile)

		verifyTar(t, tr, chMap)

	})
	t.Run("options", func(t *testing.T) {
		testFileName := "testexportfile.tar"
		defer os.RemoveAll("src")
		defer os.RemoveAll(filepath.Join(".", testFileName))

		err := os.Mkdir("src", 0775)
		if err != nil {
			t.Fatal(err)
		}

		chMap, err := createTestStore("src")
		if err != nil {
			t.Fatal(err)
		}

		updater := &checkUpdater{t: t}
		err = exporter.Export(
			"src",
			exporter.WithDestinationFilename(testFileName),
			exporter.WithProgressUpdater(updater),
		)
		if err != nil {
			t.Fatal(err)
		}

		if updater.prev != 100 {
			t.Fatal("Final update incorrect")
		}

		tarFile, err := os.Open(filepath.Join(".", testFileName))
		if err != nil {
			t.Fatal(err)
		}
		tr := tar.NewReader(tarFile)

		verifyTar(t, tr, chMap)

	})
}

func createTestStore(src string) (map[string]swarm.Chunk, error) {
	idx, closer, err := exporter.GetRetrievalIndex(src)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	chunkMap := make(map[string]swarm.Chunk, 100)
	chunks := chunktesting.GenerateTestRandomChunks(100)
	for _, c := range chunks {
		item := shed.Item{
			Address:        c.Address().Bytes(),
			Data:           c.Data(),
			StoreTimestamp: time.Now().Unix(),
		}
		err := idx.Put(item)
		if err != nil {
			return nil, err
		}
		chunkMap[c.Address().String()] = c
	}
	return chunkMap, nil
}
