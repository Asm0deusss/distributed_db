package raft

import (
	zlog "github.com/rs/zerolog/log"
	"net/http"
)

type UserRequest struct {
	Operation string
	Key       string
	Value     string
}

func (r *replica) handleUserRequest(w http.ResponseWriter, req *http.Request) {
	if r.master.Works() {
		zlog.Info().Str("addr", r.addr).Msg("Handle master request")
		r.handlerMasterRequest(w, req)
	} else {
		zlog.Info().Str("addr", r.addr).Msg("Handler replica request")
		r.handleReplicaRequest(w, req)
	}
}

func (r *replica) handlerMasterRequest(w http.ResponseWriter, req *http.Request) {
	body, err := parseRequest[UserRequest](req)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if req.Method == "GET" {
		r.handleReadRequest(w, body)
		return
	}

	r.mutex.RLock()
	newEntry := Entry{r.PersistentInfo.Term, body.Operation, body.Key, body.Value}
	logLength := r.log.Length()
	r.mutex.RUnlock()

	// Update log
	r.log.Update([]Entry{newEntry}, logLength)

	// Sync new entry with nodes
	go r.master.InitForceSync(logLength)

	w.WriteHeader(http.StatusAccepted)
}

func (r *replica) handleReplicaRequest(w http.ResponseWriter, req *http.Request) {
	if req.Method != "GET" {
		if r.PersistentInfo.Master != "" {
			if r.PersistentInfo.Master == r.addr {
				r.handlerMasterRequest(w, req)
				return
			}

			w.Header().Add("Master", r.PersistentInfo.Master)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	body, err := parseRequest[UserRequest](req)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	r.handleReadRequest(w, body)
}

func (r *replica) handleReadRequest(w http.ResponseWriter, body *UserRequest) {
	if body.Operation != "read" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	val, err := r.storage.Read(body.Key)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.Write([]byte(val))
}
