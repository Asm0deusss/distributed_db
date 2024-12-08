package raft

import (
	"encoding/json"
	"os"
)

type PersistentInfo struct {
	Filename string
	Term     int
	Master   string
}

func (pi *PersistentInfo) update(newTerm int, newMaster string) {
	pi.Term = newTerm
	pi.Master = newMaster
	pi.dumpOnDisk()
}

func (pi *PersistentInfo) readFromDisk() {
	data, err := os.ReadFile(pi.Filename)
	if err != nil {
		if os.IsNotExist(err) {
			pi.Master = ""
			pi.Term = 0
			return
		}
	}

	var dump PersistentInfo
	json.Unmarshal(data, &dump)

	pi.Master = dump.Master
	pi.Term = dump.Term
}

func (pi *PersistentInfo) dumpOnDisk() {
	data, _ := json.Marshal(pi)
	os.WriteFile(pi.Filename, data, 0666)
}
