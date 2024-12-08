package raft

import (
	"ddb/internal/storage"
	"encoding/json"
	zlog "github.com/rs/zerolog/log"
	"os"
	"sync"
)

const CREATE = "create"
const UPDATE = "update"
const DELETE = "delete"

type LogInterface interface {
	Update(entries []Entry, index int)
	Apply(applyCount int)
	TermAt(index int) int
	After(index int) []Entry
	Length() int
	LastTerm() int
	AppliedCount() int
}

type Entry struct {
	Term      int
	Operation string
	Key       string
	Value     string
}

func NewLog(filename string, storage storage.Interface) LogInterface {
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		var data []Entry
		marshalledData, _ := json.Marshal(data)
		os.WriteFile(filename, marshalledData, 0666)
	}

	data, err := os.ReadFile(filename)
	if err != nil {
		panic(err)
	}

	var entries []Entry
	err = json.Unmarshal(data, &entries)
	if err != nil {
		panic(err)
	}

	return &log{
		mutex:        sync.RWMutex{},
		filename:     filename,
		appliedCount: 0,
		entries:      entries,
		storage:      storage,
	}
}

type log struct {
	mutex sync.RWMutex

	filename     string
	appliedCount int
	entries      []Entry

	storage storage.Interface
}

func (l *log) Update(newEntries []Entry, startIndex int) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if len(l.entries) < startIndex {
		return
	}

	// Cut entries up to startIndex
	l.entries = l.entries[:startIndex]

	// Append new entries to log
	l.entries = append(l.entries, newEntries...)

	data, _ := json.Marshal(l.entries)
	os.WriteFile(l.filename, data, 0666)
}

func (l *log) Apply(applyCount int) {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	if applyCount < l.appliedCount || applyCount > len(l.entries) {
		return
	}

	go func() {
		l.mutex.Lock()
		defer l.mutex.Unlock()

		// Apply all new entries from l.appliedCount up to applyCount
		for ; l.appliedCount < applyCount; l.appliedCount++ {
			zlog.Info().Int("appliedCount", l.appliedCount).Msg("Try to apply entry")
			l.applyEntry(&l.entries[l.appliedCount])
		}
	}()
}

func (l *log) applyEntry(entry *Entry) {
	switch entry.Operation {
	case CREATE:
		l.storage.Create(entry.Key, entry.Value)
	case UPDATE:
		l.storage.Update(entry.Key, entry.Value)
	case DELETE:
		l.storage.Delete(entry.Key)
	}
}

func (l *log) TermAt(index int) int {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	if index < 0 || len(l.entries) <= index {
		return -1
	}
	return l.entries[index].Term
}

func (l *log) After(index int) []Entry {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return l.entries[index:]
}

func (l *log) Length() int {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	return len(l.entries)
}

func (l *log) LastTerm() int {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	if len(l.entries) == 0 {
		return -1
	}
	return l.entries[len(l.entries)-1].Term
}

func (l *log) AppliedCount() int {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	return l.appliedCount
}
