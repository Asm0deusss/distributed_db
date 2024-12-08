package raft

import (
	"bytes"
	"encoding/json"
	"fmt"
	zlog "github.com/rs/zerolog/log"
	"net/http"
	"sync"
	"time"
)

const heartBeatsPeriod = 500 * time.Millisecond
const requestRetryPeriod = 1 * time.Second

type MasterInterface interface {
	Start(term int)
	InitForceSync(upToEntryIndex int)
	Works() bool
	Stop()
}

func NewMaster(log LogInterface, addr string, nodes []string) MasterInterface {
	return &master{
		log:             log,
		addr:            addr,
		term:            0,
		nodes:           nodes,
		works:           false,
		mutex:           sync.RWMutex{},
		heartbeatTicker: nil,
		stopHeartbeats:  make(chan bool),
		sync:            nil,
		stopSync:        make(chan bool),
	}
}

type syncRequest struct {
	upToEntryIndex int
	syncResult     chan bool
}

type master struct {
	log   LogInterface
	mutex sync.RWMutex

	addr  string
	term  int
	nodes []string

	works bool

	heartbeatTicker *time.Ticker
	stopHeartbeats  chan bool

	sync     chan syncRequest
	stopSync chan bool
}

func (m *master) Start(term int) {
	zlog.Info().Str("addr", m.addr).Int("term", term).Msg("Start master node")
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.term = term
	m.sync = make(chan syncRequest)
	m.heartbeatTicker = time.NewTicker(heartBeatsPeriod)

	go m.handleHeartBeatsRequests()
	go m.handleSyncRequests()

	m.works = true
}

func (m *master) Stop() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.works {
		m.works = false

		m.stopHeartbeats <- true
		m.heartbeatTicker.Stop()
		m.stopSync <- true
		close(m.sync)
	}
}

func (m *master) Works() bool {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.works
}

func (m *master) InitForceSync(upToEntryIndex int) {
	m.mutex.RLock()
	if !m.works {
		m.mutex.RUnlock()
		return
	}

	zlog.Info().Str("addr", m.addr).Int("upToEntryIndex", upToEntryIndex).Msg("Init sync request")

	// Send syncRequest in channel
	synced := make(chan bool, 1)
	m.sync <- syncRequest{upToEntryIndex, synced}
	m.mutex.RUnlock()

	// On success sync apply all entries
	if <-synced {
		zlog.Info().Str("addr", m.addr).Int("apply", upToEntryIndex+1).Msg("Success sync, apply")
		m.log.Apply(upToEntryIndex + 1)
	}
}

func (m *master) handleHeartBeatsRequests() {
	for {
		select {
		case <-m.stopHeartbeats:
			return
		case <-m.heartbeatTicker.C:
		}
		m.sendHeartBeats()
	}
}

func (m *master) sendHeartBeats() {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	request := AppendEntriesRequest{
		Master:    m.addr,
		Term:      m.term,
		Entries:   []Entry{},
		PrevTerm:  m.log.TermAt(m.log.Length() - 1),
		PrevIndex: m.log.Length(),
		Applied:   m.log.AppliedCount(),
	}
	data, _ := json.Marshal(request)

	zlog.Info().Str("addr", m.addr).Int("applied", m.log.AppliedCount()).Msg("Start to send heartbeats")
	for _, nodeUrl := range m.nodes {
		go func(nodeUrl string, data []byte) {
			resp, err := http.Post(fmt.Sprintf("%s/append_entries", nodeUrl), "application/json", bytes.NewReader(data))
			if err != nil {
				return
			}

			if resp.StatusCode == http.StatusNotFound && m.log.Length() > 0 {
				// init full log sync
				go m.InitForceSync(m.log.Length() - 1)
			}

		}(nodeUrl, data)
	}
}

func (m *master) handleSyncRequests() {
	for {
		select {
		case <-m.stopSync:
			return
		case request := <-m.sync:
			zlog.Info().Str("addr", m.addr).Int("index", request.upToEntryIndex).Msg("Handle sync request")
			result := make(chan bool, len(m.nodes))

			// Update all replicas individually
			for _, url := range m.nodes {
				go func(url string) {
					result <- m.updateReplica(url, request.upToEntryIndex)
				}(url)
			}

			updated := 0

			updatedThreshold := len(m.nodes) / 2

			for range m.nodes {
				if <-result {
					updated++
				}
				if updated >= updatedThreshold {
					break
				}
			}

			zlog.Info().Str("addr", m.addr).Bool("syncResult", updated >= updatedThreshold).Msg("GOT SYNC RESULT")

			request.syncResult <- updated >= updatedThreshold
		}
	}
}

func (m *master) updateReplica(nodeUrl string, upToEntryIndex int) bool {
	// Search the largest similar log prefix
	for i := upToEntryIndex; i >= 0; i-- {
		success, isMaster := m.sendAppendEntriesRequest(nodeUrl, i)

		if !isMaster {
			m.Stop()
			return false
		}

		if success {
			return true
		}
	}

	return true
}

func (m *master) sendAppendEntriesRequest(nodeUrl string, logPrefixIndex int) (bool, bool) {
	request := AppendEntriesRequest{
		Master:    m.addr,
		Term:      m.term,
		Entries:   m.log.After(logPrefixIndex),
		PrevTerm:  m.log.LastTerm(),
		PrevIndex: logPrefixIndex - 1,
		Applied:   0,
	}

	data, _ := json.Marshal(request)

	var response *http.Response
	var err error

	for {
		response, err = http.Post(fmt.Sprintf("%s/append_entries", nodeUrl), "application/json", bytes.NewReader(data))
		if err == nil {
			break
		}
		time.Sleep(requestRetryPeriod)
	}

	// We are not the master anymore
	if response.StatusCode == http.StatusForbidden {
		return false, false
	}

	return response.StatusCode == http.StatusOK, true
}
