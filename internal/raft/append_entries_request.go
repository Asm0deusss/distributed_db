package raft

import (
	zlog "github.com/rs/zerolog/log"
	"net/http"
)

type AppendEntriesRequest struct {
	Master    string
	Term      int
	Entries   []Entry
	PrevTerm  int
	PrevIndex int
	Applied   int
}

func (r *replica) handleAppendEntriesRequest(w http.ResponseWriter, req *http.Request) {
	body, err := parseRequest[AppendEntriesRequest](req)
	if err != nil {
		panic(err.Error())
	}

	r.mutex.Lock()
	defer r.mutex.Unlock()

	// Got stale message
	if body.Term < r.PersistentInfo.Term {
		w.WriteHeader(http.StatusForbidden)
	}

	// We are not the master anymore
	if body.Term > r.PersistentInfo.Term {
		r.master.Stop()
		r.PersistentInfo.update(body.Term, body.Master)
	}

	// Got message from master, he is alive
	r.resetTimer()

	// Entries are diverged
	if body.PrevTerm >= r.log.Length() || body.PrevTerm != r.log.TermAt(body.PrevIndex) {
		w.WriteHeader(http.StatusBadRequest)
	}

	zlog.Info().Str("addr", r.addr).Int("index", body.PrevIndex+1).Msg("Update entries")

	// Update entries in Log
	r.log.Update(body.Entries, body.PrevIndex+1)

	// Apply new entries in file
	r.log.Apply(body.Applied)
	w.WriteHeader(http.StatusOK)
}
