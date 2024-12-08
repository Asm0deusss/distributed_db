package raft

import (
	"bytes"
	"ddb/internal/storage"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sync"
	"time"

	zlog "github.com/rs/zerolog/log"
)

const initElectionTime = 1500 * time.Millisecond

func parseRequest[T any](r *http.Request) (*T, error) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	var data T
	err = json.Unmarshal(body, &data)
	if err != nil {
		return nil, err
	}
	return &data, nil
}

type ReplicaInterface interface {
	Start(wg *sync.WaitGroup)
}

func NewReplica(port int, usedPorts []int) ReplicaInterface {
	var nodes []string
	for _, v := range usedPorts {
		if v != port {
			nodes = append(nodes, fmt.Sprintf("http://localhost:%d", v))
		}
	}

	nodeAddr := fmt.Sprintf("http://localhost:%d", port)
	nodeStorage := storage.NewStorage(fmt.Sprintf("data/storage_%d", port))
	nodeLog := NewLog(fmt.Sprintf("data/log_%d", port), nodeStorage)

	nodePersistentInfo := PersistentInfo{
		Filename: fmt.Sprintf("data/persistent_info_%d", port),
	}
	nodePersistentInfo.readFromDisk()

	r := &replica{
		srv:               nil,
		addr:              nodeAddr,
		nodes:             nodes,
		log:               nodeLog,
		storage:           nodeStorage,
		PersistentInfo:    nodePersistentInfo,
		master:            NewMaster(nodeLog, nodeAddr, nodes),
		initElectionTimer: nil,
	}

	srvMux := http.NewServeMux()
	srvMux.HandleFunc("/append_entries", func(w http.ResponseWriter, req *http.Request) {
		r.handleAppendEntriesRequest(w, req)
	})
	srvMux.HandleFunc("/elect_master", func(w http.ResponseWriter, req *http.Request) {
		r.handleElectMasterRequest(w, req)
	})
	srvMux.HandleFunc("/db", func(w http.ResponseWriter, req *http.Request) {
		r.handleUserRequest(w, req)
	})
	r.srv = &http.Server{Addr: nodeAddr[7:], Handler: srvMux}

	return r
}

type replica struct {
	srv   *http.Server
	addr  string
	nodes []string

	log     LogInterface
	storage storage.Interface

	mutex sync.RWMutex

	PersistentInfo PersistentInfo

	master MasterInterface

	initElectionTimer *time.Timer
}

func (r *replica) Start(wg *sync.WaitGroup) {
	wg.Add(1)
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.runTimer()

	if r.PersistentInfo.Master == r.addr {
		r.initElectionTimer.Stop()
		r.master.Start(r.PersistentInfo.Term)
		go func() {
			time.Sleep(time.Second)
			go r.master.InitForceSync(r.log.Length() - 1)
		}()
	}

	go func() {
		err := r.srv.ListenAndServe()
		fmt.Println(err.Error())
		wg.Done()
	}()
}

func (r *replica) runTimer() {
	t := time.Duration(rand.Intn(1500)*1000000) + initElectionTime

	r.initElectionTimer = time.AfterFunc(t, func() {
		r.tryElect()
	})
}

func (r *replica) resetTimer() {
	r.initElectionTimer.Stop()
	r.runTimer()
}

func (r *replica) tryElect() {
	zlog.Info().Str("addr", r.addr).Msg("Try election")

	votes := make(chan struct{}, len(r.nodes))
	votesCount := (len(r.nodes) + 1) / 2

	refuses := make(chan struct{}, len(r.nodes))
	refusesCount := len(r.nodes)/2 + 1

	stop := make(chan struct{}, 2)

	r.mutex.Lock()
	r.PersistentInfo.update(r.PersistentInfo.Term+1, r.addr)
	r.mutex.Unlock()

	request := ElectMasterRequest{
		Term:            r.PersistentInfo.Term,
		NewMasterAddr:   r.PersistentInfo.Master,
		EntriesLength:   r.log.Length(),
		EntriesLastTerm: r.log.LastTerm(),
	}

	data, _ := json.Marshal(request)

	for _, nodeUrl := range r.nodes {
		go func(nodeUrl string, data []byte) {
			response, err := http.Post(nodeUrl+"/elect_master", "application/json", bytes.NewReader(data))

			if err != nil || response.StatusCode != http.StatusOK {
				refuses <- struct{}{}
				return
			}

			votes <- struct{}{}
		}(nodeUrl, data)
	}

	election := make(chan bool, 1)
	go func() {
		for i := 0; i < votesCount; i++ {
			select {
			case <-stop:
				return
			case <-votes:
			}
		}
		election <- true
	}()
	go func() {
		for i := 0; i < refusesCount; i++ {
			select {
			case <-stop:
				return
			case <-refuses:
			}
		}
		election <- false
	}()

	go func() {
		r.mutex.Lock()
		defer r.mutex.Unlock()
		r.initElectionTimer.Stop()
	}()

	elected := <-election
	stop <- struct{}{}

	if elected {
		zlog.Info().Str("addr", r.addr).Msg("Become master")
		r.becomeMaster()
	}

	r.resetTimer()
}

func (r *replica) becomeMaster() {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.initElectionTimer.Stop()
	r.master.Start(r.PersistentInfo.Term)
}
