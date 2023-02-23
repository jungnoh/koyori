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
