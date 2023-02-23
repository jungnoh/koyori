package koyori

import (
	"github.com/pkg/errors"
	"math"
	"os"
	"strconv"
	"sync"
)

var ErrEmpty = errors.New("queue is empty")

type Queue[T any] struct {
	options       QueueOptions[T]
	firstSegment  *segment[T]
	lastSegment   *segment[T]
	segmentNumber int
	mutex         sync.Mutex
}

func (q *Queue[T]) Enqueue(item T) error {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if q.lastSegment.countOnDisk() >= q.lastSegment.capacity {
		if err := q.addSegmentLocked(); err != nil {
			return errors.Wrap(err, "failed to add new segment")
		}
	}
	return errors.Wrap(q.lastSegment.add(item), "failed to insert")
}

func (q *Queue[T]) EnqueueMany(items []T) error {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	originalLen := len(items)
	for len(items) > 0 {
		enqueueCount := len(items)
		allowedEnqueueCount := q.lastSegment.capacity - q.lastSegment.countOnDisk()
		if allowedEnqueueCount < enqueueCount {
			enqueueCount = allowedEnqueueCount
		}
		if enqueueCount > 0 {
			if err := q.lastSegment.addMany(items[0:enqueueCount]); err != nil {
				return errors.Wrap(err, "failed to enqueueMany")
			}
			items = items[enqueueCount:]
		}
		if q.lastSegment.countOnDisk() >= q.lastSegment.capacity {
			if err := q.addSegmentLocked(); err != nil {
				return errors.Wrapf(err, "failed to add new segment (added %d)", originalLen-len(items))
			}
		}
	}
	return nil
}

func (q *Queue[T]) Dequeue() (*T, error) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	item, err := q.firstSegment.remove()
	if err != nil {
		if err == errEmptySegment {
			return nil, ErrEmpty
		}
		return nil, errors.Wrap(err, "failed to dequeue from segment")
	}
	if q.firstSegment.count() > 0 {
		return item, nil
	}
	if q.firstSegment.countOnDisk() >= q.firstSegment.capacity {
		if err := q.closeFullFirstSegment(); err != nil {
			return item, err
		}
	}
	return item, nil
}

func (q *Queue[T]) DequeueMany(count int) ([]T, error) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	results := [][]T{}
	for {
		removed, err := q.firstSegment.removeMany(count)
		if err != nil {
			if err == errEmptySegment {
				break
			}
			return []T{}, errors.Wrap(err, "failed to dequeueMany")
		}
		results = append(results, removed)
		count -= len(removed)
		if count == 0 || len(removed) == 0 || q.firstSegment.countOnDisk() < q.firstSegment.capacity {
			break
		}
		if err := q.closeFullFirstSegment(); err != nil {
			return []T{}, errors.Wrap(err, "failed to close segment")
		}
	}
	if q.firstSegment.countOnDisk() >= q.firstSegment.capacity {
		if err := q.closeFullFirstSegment(); err != nil {
			return []T{}, errors.Wrap(err, "failed to close segment")
		}
	}

	lenSum := 0
	for _, v := range results {
		lenSum += len(v)
	}
	result := make([]T, lenSum)
	lenSum = 0
	for _, v := range results {
		copy(result[lenSum:], v)
		lenSum += len(v)
	}
	return result, nil
}

func (q *Queue[T]) Close() error {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if err := q.firstSegment.close(); err != nil {
		return errors.Wrap(err, "failed to close segment file")
	}
	if err := q.lastSegment.close(); err != nil {
		return errors.Wrap(err, "failed to close segment file")
	}
	return nil
}

func (q *Queue[T]) closeFullFirstSegment() error {
	if err := q.firstSegment.deleteSegment(); err != nil {
		return errors.Wrap(err, "failed to delete segment")
	}
	if q.segmentCount() == 1 {
		segment, err := newSegment(q.options.MaxObjectsPerSegment, q.segmentNumber+1, &q.options)
		if err != nil {
			return errors.Wrap(err, "failed to add new segment")
		}
		q.segmentNumber++
		q.firstSegment = &segment
		q.lastSegment = &segment
	} else if q.segmentCount() == 2 {
		q.firstSegment = q.lastSegment
	} else {
		seg, err := readSegment(q.firstSegment.segmentNumber+1, &q.options)
		if err != nil {
			return errors.Wrap(err, "error creating new segment")
		}
		q.firstSegment = &seg
	}
	return nil
}

func (q *Queue[T]) addSegmentLocked() error {
	if q.segmentCount() > 1 {
		if err := q.lastSegment.close(); err != nil {
			return errors.Wrap(err, "failed to close segment file")
		}
	}
	segment, err := newSegment(q.options.MaxObjectsPerSegment, q.segmentNumber+1, &q.options)
	if err != nil {
		return errors.Wrap(err, "failed to add new segment")
	}
	q.segmentNumber++
	q.lastSegment = &segment
	return nil
}

func (q *Queue[T]) load() error {
	if err := os.MkdirAll(q.options.FolderPath, q.options.FileMode); err != nil {
		return errors.Wrap(err, "failed to ensure folder exists")
	}
	minSegment, maxSegment, count, err := q.loadSegmentRanges()
	if err != nil {
		return errors.Wrap(err, "error while reading queue directory")
	}
	if count == 0 {
		segment, err := newSegment(q.options.MaxObjectsPerSegment, 1, &q.options)
		if err != nil {
			return errors.Wrap(err, "failed to create first segment")
		}
		q.segmentNumber = 1
		q.firstSegment = &segment
		q.lastSegment = &segment
	} else if count == 1 {
		segment, err := readSegment(minSegment, &q.options)
		if err != nil {
			return errors.Wrapf(err, "failed to read segment (#%d)", minSegment)
		}
		q.segmentNumber = minSegment
		q.firstSegment = &segment
		q.lastSegment = &segment
	} else {
		firstSegment, err := readSegment(minSegment, &q.options)
		if err != nil {
			return errors.Wrapf(err, "failed to read segment (#%d)", minSegment)
		}
		lastSegment, err := readSegment(maxSegment, &q.options)
		if err != nil {
			return errors.Wrapf(err, "failed to read segment (#%d)", maxSegment)
		}
		q.segmentNumber = maxSegment
		q.firstSegment = &firstSegment
		q.lastSegment = &lastSegment
	}
	return nil
}

func (q *Queue[T]) loadSegmentRanges() (min, max, count int, err error) {
	dir, err := os.ReadDir(q.options.FolderPath)
	if err != nil {
		err = errors.Wrap(err, "failed to read directory")
		return
	}
	min, max = math.MaxInt32, 0
	for _, entry := range dir {
		if entry.IsDir() {
			continue
		}
		nameMatch := segmentFilenameRegex.FindStringSubmatch(entry.Name())
		if len(nameMatch) == 0 {
			continue
		}
		segment, err := strconv.ParseInt(nameMatch[1], 10, 32)
		if err != nil {
			continue
		}
		count++
		if int(segment) < min {
			min = int(segment)
		}
		if int(segment) > max {
			max = int(segment)
		}
	}
	return
}

func (q *Queue[T]) segmentCount() int {
	return q.lastSegment.segmentNumber - q.firstSegment.segmentNumber + 1
}

func NewQueue[T any](options QueueOptions[T]) (Queue[T], error) {
	queue := Queue[T]{options: options}
	if err := queue.load(); err != nil {
		return Queue[T]{}, errors.Wrap(err, "error while loading queue")
	}
	return queue, nil
}
