package koyori

import (
	"encoding/binary"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"os"
	"path"
	"regexp"
	"sync"
)

var errEmptySegment = errors.New("Segment is empty")
var segmentFilenameRegex = regexp.MustCompile(`^(\d+)\.queue`)

type Segment[T any] struct {
	options       *QueueOptions[T]
	file          *os.File
	segmentNumber int
	fileLock      sync.Mutex

	objects       []T
	removeCount   int
	objectCount   int
	objectsLoaded bool
}

func (s *Segment[T]) Add(obj T) error {
	s.fileLock.Lock()
	defer s.fileLock.Unlock()
	if !s.objectsLoaded {
		if err := s.Load(true); err != nil {
			return errors.Wrap(err, "error while loading objects")
		}
	}

	buf, err := s.options.Converter.Marshal(obj)
	if err != nil {
		return errors.Wrap(err, "failed to marshal object")
	}

	bufLen := len(buf)
	bufLenBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(bufLenBytes, uint32(bufLen))
	if _, err := s.file.Write(bufLenBytes); err != nil {
		return errors.Wrap(err, "failed to write object length")
	}
	if _, err := s.file.Write(buf); err != nil {
		return errors.Wrap(err, "failed to write object")
	}

	s.objects = append(s.objects, obj)
	s.objectCount++
	if s.options.AlwaysFlush {
		err = s.flushLocked()
		return errors.Wrap(err, "failed to flushLocked")
	} else {
		return nil
	}
}

func (s *Segment[T]) Remove() (*T, error) {
	s.fileLock.Lock()
	defer s.fileLock.Unlock()
	if !s.objectsLoaded {
		if err := s.Load(true); err != nil {
			return nil, errors.Wrap(err, "error while loading objects")
		}
	}

	if s.objectCount == 0 {
		return nil, errEmptySegment
	}

	// Remove from queue first
	popped := s.objects[0]
	s.objects = s.objects[1:]
	s.objectCount--
	if _, err := s.file.Write([]byte{0, 0, 0, 0}); err != nil {
		return nil, errors.Wrap(err, "failed to write deletion to disk")
	}
	s.removeCount++
	if s.options.AlwaysFlush {
		err := s.flushLocked()
		return &popped, errors.Wrap(err, "failed to flushLocked")
	} else {
		return &popped, nil
	}
}

func (s *Segment[T]) Count() int {
	s.fileLock.Lock()
	defer s.fileLock.Unlock()
	return s.objectCount
}

func (s *Segment[T]) CountOnDisk() int {
	s.fileLock.Lock()
	defer s.fileLock.Unlock()

	return s.objectCount + s.removeCount
}

func (s *Segment[T]) openFileForWrite(additionalFlags int) error {
	file, err := os.OpenFile(s.filePath(), os.O_APPEND|os.O_WRONLY|additionalFlags, s.options.FileMode)
	if err != nil {
		return errors.Wrap(err, "failed to open Segment file")
	}
	s.file = file
	return nil
}

func (s *Segment[T]) flushLocked() error {
	return errors.Wrap(s.file.Sync(), "failed to sync file")
}

func (s *Segment[T]) Load(loadObjects bool) error {
	if err := s.load(loadObjects); err != nil {
		return err
	}
	s.objectsLoaded = loadObjects
	return s.openFileForWrite(0)
}

func (s *Segment[T]) load(loadObjects bool) error {
	if s.file != nil {
		if err := s.file.Close(); err != nil {
			return errors.Wrap(err, "failed to Close existing file")
		}
	}
	s.removeCount = 0
	s.objects = []T{}
	s.objectCount = 0

	if file, err := os.OpenFile(s.filePath(), os.O_RDONLY, os.ModePerm); err == nil {
		s.file = file
		defer s.file.Close()
	} else {
		return errors.Wrap(err, "failed to open file")
	}

	for {
		lengthBuf := make([]byte, 4)
		if n, err := io.ReadFull(s.file, lengthBuf); err != nil {
			if err == io.EOF {
				break
			}
			return errors.Wrapf(err, "error reading object length bytes (read %d bytes)", n)
		}
		length := binary.LittleEndian.Uint32(lengthBuf)
		if length == 0 {
			if s.objectCount == 0 {
				return errors.New("Found deletion marker, but no objects are left")
			}
			if loadObjects {
				s.objects = s.objects[1:]
			}
			s.removeCount++
			s.objectCount--
		} else {
			if loadObjects {
				buf := make([]byte, length)
				if n, err := io.ReadFull(s.file, buf); err != nil {
					return errors.Wrapf(err, "error reading object (read %d bytes)", n)
				}
				obj, err := s.options.Converter.Unmarshal(buf)
				if err != nil {
					return errors.Wrap(err, "failed to unmarshal object")
				}
				s.objects = append(s.objects, obj)
			} else {
				if _, err := s.file.Seek(int64(length), io.SeekCurrent); err != nil {
					return errors.Wrapf(err, "error seeking %d bytes", length)
				}
			}
			s.objectCount++
		}
	}

	return nil
}

func (s *Segment[T]) Close() error {
	s.fileLock.Lock()
	defer s.fileLock.Unlock()

	return s.file.Close()
}

func (s *Segment[T]) DeleteSelf() error {
	if err := s.file.Close(); err != nil {
		return errors.Wrap(err, "failed to Close file")
	}
	return errors.Wrap(os.Remove(s.filePath()), "failed to delete file")
}

func (s *Segment[T]) filePath() string {
	return path.Join(s.options.FolderPath, s.filename())
}

func (s *Segment[T]) filename() string {
	return fmt.Sprintf("%05d.queue", s.segmentNumber)
}

func NewSegment[T any](SegmentNumber int, options *QueueOptions[T]) (Segment[T], error) {
	seg := Segment[T]{
		options:       options,
		segmentNumber: SegmentNumber,
	}
	return seg, errors.Wrap(seg.openFileForWrite(os.O_TRUNC|os.O_CREATE), "failed to create Segment file")
}

func OpenSegment[T any](loadObjects bool, SegmentNumber int, options *QueueOptions[T]) (Segment[T], error) {
	seg := Segment[T]{
		options:       options,
		segmentNumber: SegmentNumber,
	}
	if err := seg.Load(loadObjects); err != nil {
		return Segment[T]{}, errors.Wrap(err, "failed to read Segment file")
	}
	return seg, errors.Wrap(seg.openFileForWrite(0), "failed to open Segment file")
}
