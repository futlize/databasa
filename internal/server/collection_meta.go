package server

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
)

const (
	collectionMetaFile    = "collection.meta"
	collectionMetaVersion = 1
)

var collectionMetaMagic = [8]byte{'K', 'C', 'O', 'L', 'L', 'M', 'E', '1'}

type collectionMeta struct {
	Name        string
	Dimension   uint32
	Metric      string
	Compression string
	ShardCount  uint32
}

func saveCollectionMeta(dir string, meta collectionMeta) error {
	metricCode, ok := metricCode(meta.Metric)
	if !ok {
		return fmt.Errorf("unsupported metric %q", meta.Metric)
	}
	compressionCode, ok := compressionCode(meta.Compression)
	if !ok {
		return fmt.Errorf("unsupported compression %q", meta.Compression)
	}
	if meta.ShardCount == 0 {
		return fmt.Errorf("shard count must be > 0")
	}
	nameBytes := []byte(meta.Name)
	if len(nameBytes) > math.MaxUint16 {
		return fmt.Errorf("collection name too long")
	}

	var buf bytes.Buffer
	buf.Write(collectionMetaMagic[:])
	if err := binary.Write(&buf, binary.LittleEndian, uint16(collectionMetaVersion)); err != nil {
		return err
	}
	if err := binary.Write(&buf, binary.LittleEndian, meta.Dimension); err != nil {
		return err
	}
	if err := buf.WriteByte(metricCode); err != nil {
		return err
	}
	if err := buf.WriteByte(compressionCode); err != nil {
		return err
	}
	if err := binary.Write(&buf, binary.LittleEndian, meta.ShardCount); err != nil {
		return err
	}
	if err := binary.Write(&buf, binary.LittleEndian, uint16(len(nameBytes))); err != nil {
		return err
	}
	if _, err := buf.Write(nameBytes); err != nil {
		return err
	}

	path := filepath.Join(dir, collectionMetaFile)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, buf.Bytes(), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func loadCollectionMeta(dir string) (collectionMeta, error) {
	path := filepath.Join(dir, collectionMetaFile)
	f, err := os.Open(path)
	if err != nil {
		return collectionMeta{}, err
	}
	defer f.Close()

	var magic [8]byte
	if _, err := io.ReadFull(f, magic[:]); err != nil {
		return collectionMeta{}, err
	}
	if magic != collectionMetaMagic {
		return collectionMeta{}, fmt.Errorf("invalid collection meta format")
	}

	var version uint16
	if err := binary.Read(f, binary.LittleEndian, &version); err != nil {
		return collectionMeta{}, err
	}
	if version != collectionMetaVersion {
		return collectionMeta{}, fmt.Errorf("unsupported collection meta version %d", version)
	}

	var dim uint32
	if err := binary.Read(f, binary.LittleEndian, &dim); err != nil {
		return collectionMeta{}, err
	}

	var metricCode [1]byte
	var compressionCode [1]byte
	if _, err := io.ReadFull(f, metricCode[:]); err != nil {
		return collectionMeta{}, err
	}
	if _, err := io.ReadFull(f, compressionCode[:]); err != nil {
		return collectionMeta{}, err
	}

	var shardCount uint32
	if err := binary.Read(f, binary.LittleEndian, &shardCount); err != nil {
		return collectionMeta{}, err
	}

	var nameLen uint16
	if err := binary.Read(f, binary.LittleEndian, &nameLen); err != nil {
		return collectionMeta{}, err
	}
	nameBytes := make([]byte, nameLen)
	if _, err := io.ReadFull(f, nameBytes); err != nil {
		return collectionMeta{}, err
	}

	metric, ok := metricFromCode(metricCode[0])
	if !ok {
		return collectionMeta{}, fmt.Errorf("invalid metric code %d", metricCode[0])
	}
	compression, ok := compressionFromCode(compressionCode[0])
	if !ok {
		return collectionMeta{}, fmt.Errorf("invalid compression code %d", compressionCode[0])
	}
	if shardCount == 0 {
		return collectionMeta{}, fmt.Errorf("invalid shard count 0")
	}

	return collectionMeta{
		Name:        string(nameBytes),
		Dimension:   dim,
		Metric:      metric,
		Compression: compression,
		ShardCount:  shardCount,
	}, nil
}

func metricCode(metric string) (byte, bool) {
	switch metric {
	case "cosine":
		return 1, true
	case "l2":
		return 2, true
	case "dot_product":
		return 3, true
	default:
		return 0, false
	}
}

func metricFromCode(code byte) (string, bool) {
	switch code {
	case 1:
		return "cosine", true
	case 2:
		return "l2", true
	case 3:
		return "dot_product", true
	default:
		return "", false
	}
}

func compressionCode(compression string) (byte, bool) {
	switch compression {
	case "none":
		return 0, true
	case "int8":
		return 1, true
	default:
		return 0, false
	}
}

func compressionFromCode(code byte) (string, bool) {
	switch code {
	case 0:
		return "none", true
	case 1:
		return "int8", true
	default:
		return "", false
	}
}
