package memstash

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"
)

var (
	// ErrBadSnapshot means the stream is not a memstash snapshot, carries a version this build cannot read, or ends
	// early. Items decoded before the failure stay in the cache.
	ErrBadSnapshot = errors.New("memstash: corrupt or unrecognized snapshot")
	// ErrNilCodec is returned by SaveTo and LoadFrom when a codec argument is nil.
	ErrNilCodec = errors.New("memstash: codec must not be nil")
	// ErrConflictingLoadOptions is returned when LoadFrom is given both LoadWithTTL and LoadWithCurrentTTL.
	ErrConflictingLoadOptions = errors.New("memstash: LoadWithTTL and LoadWithCurrentTTL are mutually exclusive")
)

// LoadOption tunes LoadFrom.
type LoadOption uint8

const (
	// LoadWithCurrentTTL gives every loaded item a full, fresh lifetime, exactly as Set would. The default.
	LoadWithCurrentTTL LoadOption = iota
	// LoadWithTTL restores the expiration the item carried when the snapshot was taken: it expires at the same moment
	// it would have, so time spent in the file counts against it and items already past that moment are skipped.
	LoadWithTTL
	// LoadToL2 also writes every loaded item to the second level, following WritePolicy. Without an L2 it does nothing.
	LoadToL2
)

const (
	snapshotMagic   = "memstash"
	snapshotVersion = 2
	// snapshotHeaderSize is the magic, the version byte and the save timestamp.
	snapshotHeaderSize = len(snapshotMagic) + 1 + 8
	// maxSnapshotChunk bounds one encoded key or value so a corrupt length cannot turn into an arbitrary allocation.
	maxSnapshotChunk = 1 << 30
)

// SaveTo writes the first level to w: every live item as a key/value pair, encoded with the given codecs, plus how
// long it had left to live. The stream carries the moment it was taken, so LoadWithTTL can put the expirations back
// on the same absolute schedule. L2 is not read.
//
// The walk is the lock-free one Iterator uses, so the snapshot is weakly consistent: an item written or removed while
// SaveTo runs may or may not appear, but no pair is ever torn. On a codec or writer error the stream is left
// incomplete and the error is returned.
func (c *Cache[K, V]) SaveTo(w io.Writer, keys Codec[K], values Codec[V]) error {
	if keys == nil || values == nil {
		return ErrNilCodec
	}
	savedAt := time.Now()
	buffered := bufio.NewWriter(w)
	header := make([]byte, 0, snapshotHeaderSize)
	header = append(header, snapshotMagic...)
	header = append(header, snapshotVersion)
	header = binary.BigEndian.AppendUint64(header, uint64(savedAt.UnixNano()))
	if _, err := buffered.Write(header); err != nil {
		return err
	}

	var (
		chunk   [binary.MaxVarintLen64]byte
		saveErr error
	)
	c.iterateWithLife(func(key K, value V, life time.Duration) bool {
		keyBytes, err := keys.Marshal(key)
		if err != nil {
			saveErr = fmt.Errorf("memstash: encoding a key: %w", err)
			return false
		}
		valueBytes, err := values.Marshal(value)
		if err != nil {
			saveErr = fmt.Errorf("memstash: encoding a value: %w", err)
			return false
		}
		// Key lengths are stored biased by one, which frees 0 to mark the end of the stream.
		if saveErr = writeChunk(buffered, chunk[:], uint64(len(keyBytes))+1, keyBytes); saveErr != nil {
			return false
		}
		if saveErr = writeChunk(buffered, chunk[:], uint64(len(valueBytes)), valueBytes); saveErr != nil {
			return false
		}
		// Lifetimes are seconds from savedAt, biased by one so 0 can mean "no TTL".
		var lifeSeconds uint64
		if life > 0 {
			lifeSeconds = uint64((life+time.Second-1)/time.Second) + 1
		}
		saveErr = writeChunk(buffered, chunk[:], lifeSeconds, nil)
		return saveErr == nil
	})
	if saveErr != nil {
		return saveErr
	}
	if err := writeChunk(buffered, chunk[:], 0, nil); err != nil {
		return err
	}
	return buffered.Flush()
}

// LoadFrom reads a snapshot written by SaveTo into the first level. Items are stored the way Set stores them, so the
// cost function, the capacity and the eviction policy all apply: a snapshot larger than the cache loads only as much
// as fits. Lifetimes come out fresh unless LoadWithTTL asks for the originals; L2 is left alone unless LoadToL2 is
// given, and only then is ctx used.
//
// Existing items are kept; a key present in both is overwritten by the snapshot. On a truncated or corrupt stream the
// items read so far stay in the cache and the error is returned.
func (c *Cache[K, V]) LoadFrom(
	ctx context.Context, r io.Reader, keys Codec[K], values Codec[V], opts ...LoadOption,
) error {
	if keys == nil || values == nil {
		return ErrNilCodec
	}
	var keepTTL, freshTTL, toL2 bool
	for _, opt := range opts {
		switch opt {
		case LoadWithTTL:
			keepTTL = true
		case LoadWithCurrentTTL:
			freshTTL = true
		case LoadToL2:
			toL2 = true
		}
	}
	if keepTTL && freshTTL {
		return ErrConflictingLoadOptions
	}
	toL2 = toL2 && c.l2Cache != nil

	buffered := bufio.NewReader(r)
	savedAt, err := readHeader(buffered)
	if err != nil {
		return err
	}

	for {
		keyLen, err := binary.ReadUvarint(buffered)
		if err != nil {
			return fmt.Errorf("%w: reading a key length: %w", ErrBadSnapshot, err)
		}
		if keyLen == 0 {
			return nil // the end-of-stream marker: a complete snapshot
		}
		keyBytes, err := readChunk(buffered, keyLen-1)
		if err != nil {
			return err
		}
		valueLen, err := binary.ReadUvarint(buffered)
		if err != nil {
			return fmt.Errorf("%w: reading a value length: %w", ErrBadSnapshot, err)
		}
		valueBytes, err := readChunk(buffered, valueLen)
		if err != nil {
			return err
		}
		lifeSeconds, err := binary.ReadUvarint(buffered)
		if err != nil {
			return fmt.Errorf("%w: reading a lifetime: %w", ErrBadSnapshot, err)
		}

		key, err := keys.Unmarshal(keyBytes)
		if err != nil {
			return fmt.Errorf("memstash: decoding a key: %w", err)
		}
		value, err := values.Unmarshal(valueBytes)
		if err != nil {
			return fmt.Errorf("memstash: decoding a value: %w", err)
		}

		expireOff, ttl := c.expireOffset(), c.ttl
		if keepTTL && lifeSeconds > 0 {
			remaining := time.Until(savedAt.Add(time.Duration(lifeSeconds-1) * time.Second))
			if remaining <= 0 {
				continue // it would already have expired: nothing to restore
			}
			expireOff, ttl = c.expireOffsetIn(remaining), remaining
		}
		c.setMemory(key, value, expireOff)
		if toL2 {
			if err := c.writeLoadedToL2(ctx, key, value, ttl); err != nil {
				return err
			}
		}
	}
}

// writeLoadedToL2 mirrors what Set does after the memory write, with the lifetime the loaded item ended up with.
func (c *Cache[K, V]) writeLoadedToL2(ctx context.Context, key K, value V, ttl time.Duration) error {
	switch c.l2WritePolicy {
	case WriteThrough:
		return c.l2Cache.Set(ctx, key, value, ttl)
	case WriteBack:
		c.enqueueWriteBack(key, value)
	}
	return nil
}

func readHeader(r *bufio.Reader) (savedAt time.Time, err error) {
	header := make([]byte, snapshotHeaderSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return time.Time{}, fmt.Errorf("%w: reading the header: %w", ErrBadSnapshot, err)
	}
	if string(header[:len(snapshotMagic)]) != snapshotMagic {
		return time.Time{}, fmt.Errorf("%w: bad magic", ErrBadSnapshot)
	}
	if version := header[len(snapshotMagic)]; version != snapshotVersion {
		return time.Time{}, fmt.Errorf("%w: version %d, want %d", ErrBadSnapshot, version, snapshotVersion)
	}
	nanos := binary.BigEndian.Uint64(header[len(snapshotMagic)+1:])
	return time.Unix(0, int64(nanos)), nil
}

func writeChunk(w *bufio.Writer, header []byte, size uint64, payload []byte) error {
	n := binary.PutUvarint(header, size)
	if _, err := w.Write(header[:n]); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

func readChunk(r *bufio.Reader, size uint64) ([]byte, error) {
	if size > maxSnapshotChunk {
		return nil, fmt.Errorf("%w: chunk of %d bytes", ErrBadSnapshot, size)
	}
	chunk := make([]byte, size)
	if _, err := io.ReadFull(r, chunk); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrBadSnapshot, err)
	}
	return chunk, nil
}
