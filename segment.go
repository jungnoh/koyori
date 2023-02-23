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

var errEmptySegment = errors.New("segment is empty")
var segmentFilenameRegex = regexp.MustCompile(`^(\d+)\.queue`)

type segment[T any] struct {
	folderPath    string
	segmentNumber int
	file          *os.File
	converter     Converter[T]
	removeCount   int
	objects       []T
	fileLock      sync.Mutex
	options       *QueueOptions[T]
}

func (s *segment[T]) add(obj T) error {
	s.fileLock.Lock()
	defer s.fileLock.Unlock()

	buf, err := s.converter.Marshal(obj)
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
	if s.options.AlwaysFlush {
		err = s.flushLocked()
		return errors.Wrap(err, "failed to flushLocked")
	} else {
		return nil
	}
}

func (s *segment[T]) remove() (*T, error) {
	s.fileLock.Lock()
	defer s.fileLock.Unlock()

	if len(s.objects) == 0 {
		return nil, errEmptySegment
	}

	// Remove from queue first
	popped := s.objects[0]
	s.objects = s.objects[1:]
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

func (s *segment[T]) count() int {
	s.fileLock.Lock()
	defer s.fileLock.Unlock()

	return len(s.objects)
}

func (s *segment[T]) countOnDisk() int {
	s.fileLock.Lock()
	defer s.fileLock.Unlock()

	return len(s.objects) + s.removeCount
}

func (s *segment[T]) flushLocked() error {
	return errors.Wrap(s.file.Sync(), "failed to sync file")
}

func (s *segment[T]) load() error {
	s.fileLock.Lock()
	defer s.fileLock.Unlock()

	if s.file != nil {
		if err := s.file.Close(); err != nil {
			return errors.Wrap(err, "failed to close existing file")
		}
	}
	s.removeCount = 0
	s.objects = []T{}

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
			if len(s.objects) == 0 {
				return errors.New("Found deletion marker, but no objects are left")
			}
			s.objects = s.objects[1:]
			s.removeCount++
		} else {
			buf := make([]byte, length)
			if n, err := io.ReadFull(s.file, buf); err != nil {
				return errors.Wrapf(err, "error reading object (read %d bytes)", n)
			}
			obj, err := s.converter.Unmarshal(buf)
			if err != nil {
				return errors.Wrap(err, "failed to unmarshal object")
			}
			s.objects = append(s.objects, obj)
		}
	}
	return nil
}

func (s *segment[T]) close() error {
	s.fileLock.Lock()
	defer s.fileLock.Unlock()

	return s.file.Close()
}

func (s *segment[T]) deleteSegment() error {
	if err := s.file.Close(); err != nil {
		return errors.Wrap(err, "failed to close file")
	}
	return errors.Wrap(os.Remove(s.filePath()), "failed to delete file")
}

func (s *segment[T]) filePath() string {
	return path.Join(s.folderPath, s.filename())
}

func (s *segment[T]) filename() string {
	return fmt.Sprintf("%05d.queue", s.segmentNumber)
}

func newSegment[T any](segmentNumber int, options *QueueOptions[T]) (segment[T], error) {
	seg := segment[T]{
		folderPath:    options.FolderPath,
		segmentNumber: segmentNumber,
		converter:     options.Converter,
		options:       options,
	}
	file, err := os.OpenFile(seg.filePath(), os.O_APPEND|os.O_CREATE|os.O_TRUNC|os.O_WRONLY, seg.options.FileMode)
	if err != nil {
		return segment[T]{}, errors.Wrap(err, "failed to create segment file")
	}
	seg.file = file

	return seg, nil
}

func readSegment[T any](segmentNumber int, options *QueueOptions[T]) (segment[T], error) {
	seg := segment[T]{
		folderPath:    options.FolderPath,
		segmentNumber: segmentNumber,
		converter:     options.Converter,
		options:       options,
	}
	if err := seg.load(); err != nil {
		return segment[T]{}, errors.Wrap(err, "failed to read segment file")
	}
	file, err := os.OpenFile(seg.filePath(), os.O_APPEND|os.O_WRONLY, seg.options.FileMode)
	if err != nil {
		return segment[T]{}, errors.Wrap(err, "failed to open segment file")
	}
	seg.file = file
	return seg, nil
}