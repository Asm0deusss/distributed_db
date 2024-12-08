package raft

import (
	zlog "github.com/rs/zerolog/log"
	"net/http"
)

type ElectMasterRequest struct {
	Term            int
	NewMasterAddr   string
	EntriesLength   int
	EntriesLastTerm int
}

func (r *replica) handleElectMasterRequest(w http.ResponseWriter, req *http.Request) {
	body, err := parseRequest[ElectMasterRequest](req)
	if err != nil {
		panic(err.Error())
	}

	r.mutex.Lock()
	defer r.mutex.Unlock()

	// Got stale message
	if body.Term < r.PersistentInfo.Term {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	// Master already elected
	if body.Term == r.PersistentInfo.Term && r.PersistentInfo.Master != "" {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	// Got stale log on candidate
	if body.EntriesLength < r.log.Length() {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	// Logs diver
	if body.EntriesLength == r.log.Length() && body.EntriesLastTerm < r.log.LastTerm() {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	if r.master.Works() {
		r.master.Stop()
	}

	r.resetTimer()

	// Update persistent info
	r.PersistentInfo.update(body.Term, body.NewMasterAddr)

	zlog.Info().Str("addr", r.addr).Msg("Got new master")

	w.WriteHeader(http.StatusOK)
}
