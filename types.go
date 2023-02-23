package koyori

type Converter[T any] interface {
	Marshal(obj T) ([]byte, error)
	Unmarshal(data []byte) (T, error)
}
