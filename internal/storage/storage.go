package storage

import (
	"encoding/json"
	"errors"
	"os"
)

type Interface interface {
	Create(key string, value string)
	Read(key string) (string, error)
	Update(key string, value string)
	Delete(key string)
}

type Storage struct {
	Filename string
	Data     map[string]string
}

func NewStorage(filename string) Interface {
	data := map[string]string{}
	marshalledData, _ := json.Marshal(data)
	os.WriteFile(filename, marshalledData, 0666)
	return &Storage{filename, map[string]string{}}
}

func (s *Storage) Create(key string, value string) {
	s.Data[key] = value
	s.dump()
}

func (s *Storage) Read(key string) (string, error) {
	val, ok := s.Data[key]
	if !ok {
		return "", errors.New("key not found")
	}

	return val, nil
}

func (s *Storage) Update(key string, value string) {
	s.Data[key] = value
	s.dump()
}

func (s *Storage) Delete(key string) {
	delete(s.Data, key)
	s.dump()
}

func (s *Storage) dump() {
	result, err := json.Marshal(s.Data)
	if err != nil {
		panic(err)
	}
	err = os.WriteFile(s.Filename, result, 0666)
	if err != nil {
		panic(err)
	}
}
