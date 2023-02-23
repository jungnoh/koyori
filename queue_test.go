package koyori_test

import (
	"fmt"
	"github.com/jungnoh/koyori"
	"github.com/stretchr/testify/assert"
	"os"
	"path"
	"testing"
	"time"
)

type StringConverter struct{}

func (s StringConverter) Marshal(v string) ([]byte, error) {
	return []byte(v), nil
}

func (s StringConverter) Unmarshal(v []byte) (string, error) {
	return string(v), nil
}

func assertDequeue[T any](t *testing.T, queue *koyori.Queue[T], expected T) {
	item, err := queue.Dequeue()
	assert.Nil(t, err)
	assert.Equal(t, expected, *item)
}

func assertDequeueMany[T any](t *testing.T, queue *koyori.Queue[T], count int, expected []T) {
	items, err := queue.DequeueMany(count)
	assert.Nil(t, err)
	assert.Equal(t, expected, items)
}

func TestQueueBasicInsert(t *testing.T) {
	queue, err := koyori.NewQueue(koyori.QueueOptions[string]{
		Converter:            StringConverter{},
		FolderPath:           path.Join(os.TempDir(), fmt.Sprintf("%d", time.Now().UnixNano())),
		FileMode:             os.ModePerm,
		MaxObjectsPerSegment: 2,
	})
	assert.Nil(t, err)

	assert.Nil(t, queue.Enqueue("a"))
	assert.Nil(t, queue.Enqueue("b"))
	assert.Nil(t, queue.Enqueue("c"))
	assert.Nil(t, queue.Enqueue("d"))
	assertDequeue(t, &queue, "a")
	assertDequeue(t, &queue, "b")
	assertDequeue(t, &queue, "c")
	assert.Nil(t, queue.Enqueue("e"))
	assertDequeue(t, &queue, "d")
	assertDequeue(t, &queue, "e")
	_, err = queue.Dequeue()
	assert.Equal(t, koyori.ErrEmpty, err)
}

func TestQueuePersist(t *testing.T) {
	opts := koyori.QueueOptions[string]{
		Converter:            StringConverter{},
		FolderPath:           path.Join(os.TempDir(), fmt.Sprintf("%d", time.Now().UnixNano())),
		FileMode:             os.ModePerm,
		MaxObjectsPerSegment: 2,
	}

	queue, err := koyori.NewQueue(opts)
	assert.Nil(t, err)

	assert.Nil(t, queue.Enqueue("a"))
	assert.Nil(t, queue.Enqueue("b"))
	assert.Nil(t, queue.Enqueue("c"))
	assert.Nil(t, queue.Enqueue("d"))
	assert.Nil(t, queue.Enqueue("e"))
	assert.Nil(t, queue.Close())

	queue, err = koyori.NewQueue(opts)
	assert.Nil(t, err)
	assertDequeue(t, &queue, "a")
	assertDequeue(t, &queue, "b")
	assertDequeue(t, &queue, "c")
	assertDequeue(t, &queue, "d")
	assertDequeue(t, &queue, "e")
}

func TestQueueBatch(t *testing.T) {
	opts := koyori.QueueOptions[string]{
		Converter:            StringConverter{},
		FolderPath:           path.Join(os.TempDir(), fmt.Sprintf("%d", time.Now().UnixNano())),
		FileMode:             os.ModePerm,
		MaxObjectsPerSegment: 2,
	}

	queue, err := koyori.NewQueue(opts)
	assert.Nil(t, err)

	assert.Nil(t, queue.EnqueueMany([]string{"a", "b", "c", "d", "e"}))
	assertDequeueMany(t, &queue, 2, []string{"a", "b"})
	assertDequeueMany(t, &queue, 4, []string{"c", "d", "e"})

	assert.Nil(t, queue.EnqueueMany([]string{"a", "b", "c", "d", "e", "f"}))
	assertDequeueMany(t, &queue, 3, []string{"a", "b", "c"})
	assertDequeue(t, &queue, "d")
	assertDequeueMany(t, &queue, 1, []string{"e"})
	assert.Nil(t, queue.EnqueueMany([]string{"g"}))
	assertDequeueMany(t, &queue, 2, []string{"f", "g"})
}

func TestQueueCapacityChange(t *testing.T) {
	opts := koyori.QueueOptions[string]{
		Converter:            StringConverter{},
		FolderPath:           path.Join(os.TempDir(), fmt.Sprintf("%d", time.Now().UnixNano())),
		FileMode:             os.ModePerm,
		MaxObjectsPerSegment: 2,
	}

	queue, err := koyori.NewQueue(opts)
	assert.Nil(t, err)
	assert.Nil(t, queue.EnqueueMany([]string{"a", "b", "c", "d", "e"}))
	assert.Nil(t, queue.Close())

	opts.MaxObjectsPerSegment = 5
	queue, err = koyori.NewQueue(opts)
	assert.Nil(t, err)
	assertDequeueMany(t, &queue, 2, []string{"a", "b"})
	assert.Nil(t, queue.EnqueueMany([]string{"a", "b", "c", "d", "e"}))
	assertDequeueMany(t, &queue, 4, []string{"c", "d", "e", "a"})
	assertDequeueMany(t, &queue, 3, []string{"b", "c", "d"})
	assertDequeueMany(t, &queue, 2, []string{"e"})
}
