package koyori

import "os"

type QueueOptions[T any] struct {
	FolderPath           string
	AlwaysFlush          bool
	MaxObjectsPerSegment int
	FileMode             os.FileMode
	Converter            Converter[T]
}
