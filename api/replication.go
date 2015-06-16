package api

import (
	"bytes"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"github.com/cabify/couchdb-utils/util"
	"io"
)

type ReplicationConfig struct {
	ID           string  `json:"_id,omitempty"`
	REV          string  `json:"_rev,omitempty"`
	Source       string  `json:"source"`
	Target       string  `json:"target"`
	Cancel       bool    `json:"cancel"`
	CreateTarget bool    `json:"create_target"`
	Continuous   bool    `json:"continuous"`
	UserCtx      UserCtx `json:"user_ctx"` // see api/session

	Push bool `json:"-"` // Cause reversal of Source and Target
}

func (r ReplicationConfig) hasId() bool {
	return r.ID != ""
}

func (r ReplicationConfig) toJson() (io.Reader, error) {
	jsonBody, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(jsonBody), nil
}

func (r *ReplicationConfig) uniqueName() string {
	return r.Source + r.Target
}

func (r *ReplicationConfig) GenerateId() {
	hasher := md5.New()
	hasher.Write([]byte(r.uniqueName()))
	r.ID = fmt.Sprintf("%x", hasher.Sum(nil))
}

func (r ReplicationConfig) path() string {
	if r.REV == "" {
		return fmt.Sprintf("_replicator/%s", r.ID)
	} else {
		return fmt.Sprintf("_replicator/%s?rev=%s", r.ID, r.REV)
	}
}

type Replicator struct {
	ReplicationConfig
	// following fields are set after doc creation
	Owner                string `json:"owner,omitempty"`
	ReplicationId        string `json:"_replication_id,omitempty"`
	ReplicationState     string `json:"_replication_state,omitempty"`
	ReplicationStateTime string `json:"_replication_state_time,omitempty"`
}

func (r Replicator) PP(printer util.Printer) {
	printer.Print("[%s]", r.ID)
	printer.Print(" %s → %s", r.Source, r.Target)
	printer.Print(" Replication State: %s", r.ReplicationState)
	printer.Print(" Continuous: %v", r.Continuous)
	printer.Print(" Create Target: %v", r.CreateTarget)
	printer.Print(" Replication State Time: %v", r.ReplicationStateTime)
}

type Replicators map[string][]*Replicator

func (r Replicators) PP(printer util.Printer) {
	for _, replicators := range r {
		for _, replicator := range replicators {
			replicator.PP(printer)
		}
	}
}

func (r Replicators) findById(id string) (*Replicator, bool) {
	for _, replicators := range r {
		for _, replicator := range replicators {
			if replicator.ID == id {
				return replicator, true
			}
		}
	}
	return nil, false
}

func (r *Replicators) add(replicator Replicator) {
	dePtr := *r
	dePtr[replicator.ReplicationId] = append(dePtr[replicator.ReplicationId], &replicator)
}

type replicatorDocs struct {
	TotalRows int `json:"total_rows"`
	Offset    int `json:"offset"`
	Rows      []struct {
		Id         string     `json:"id"`
		Replicator Replicator `json:"doc"`
	} `json:"rows"`
}

func (r replicatorDocs) path() string {
	return "_replicator/_all_docs?include_docs=true"
}

func (r replicatorDocs) Replicators() *Replicators {
	replicators := make(Replicators)
	var ignoredPrefix = "_"
	for _, row := range r.Rows {
		if len(row.Id) > 0 && row.Id[0] == ignoredPrefix[0] {
			continue
		}
		replicators.add(row.Replicator)
	}
	return &replicators
}

func (c Couchdb) GetReplicators() (*Replicators, error) {
	docs := new(replicatorDocs)
	err := c.getJson(docs, docs.path())
	if err != nil {
		return nil, err
	}
	return docs.Replicators(), nil

}

func (c Couchdb) GetReplicator(id string) (*Replicator, error) {
	replicator := new(Replicator)
	replicator.ID = id
	err := c.getJson(replicator, replicator.path())
	return replicator, err
}

func (c Couchdb) Replicate(conf ReplicationConfig) error {
	jsonBody, err := conf.toJson()
	if err != nil {
		return err
	}
	if !conf.hasId() {
		conf.GenerateId()
	}
	jsonObj := new(interface{})
	// newschool, creating doc in /_replicator
	return c.putJson(jsonObj, jsonBody, conf.path())
}

func (c Couchdb) ReplicateHost(remoteCouch *Couchdb, conf ReplicationConfig) (*Databases, error) {
	// Determine which source to use for databases
	var masterCouch *Couchdb
	if conf.Push {
		masterCouch = &c
	} else {
		masterCouch = remoteCouch
	}

	// Grab the list of databases to sync
	var replicatedDbs Databases
	databases, err := masterCouch.GetDatabases()
	if err != nil {
		return &replicatedDbs, err
	}
	replicators, err := c.GetReplicators()
	if err != nil {
		return &replicatedDbs, err
	}
	session, err := c.GetSession()
	if err != nil {
		return &replicatedDbs, err
	}
	invalidPrefix := uint8('_')
	for _, db := range databases {
		dbName := *db.Name
		if dbName[0] == invalidPrefix {
			continue
		}
		// Swap the source and target if required
		if conf.Push {
			conf.Source = dbName
			conf.Target = remoteCouch.url(dbName)
		} else {
			conf.Source = remoteCouch.url(dbName)
			conf.Target = dbName
		}
		conf.UserCtx = session.UserCtx
		conf.GenerateId()
		existingReplicator, found := replicators.findById(conf.ID)
		if found {
			if existingReplicator.ReplicationState == "triggered" {
				// not possible to update triggered replicators
				continue
			} else {
				conf.REV = existingReplicator.REV
			}
		} else {
			conf.REV = ""
		}
		err = c.Replicate(conf)
		if err != nil {
			return &replicatedDbs, err
		}
		replicatedDbs = append(replicatedDbs, db)
	}
	return &replicatedDbs, nil
}

func (c Couchdb) DeleteReplicator(id string) error {
	replicator, err := c.GetReplicator(id)
	if err != nil {
		return err
	}
	body, err := c.del(replicator.path())
	body.Close()
	return err
}

func (c Couchdb) DeleteAllReplicators() (*Replicators, error) {
	replicators, err := c.GetReplicators()
	if err != nil {
		return replicators, err
	}
	for _, subReplicators := range *replicators {
		for _, replicator := range subReplicators {
			err := c.DeleteReplicator(replicator.ID)
			if err != nil {
				return replicators, err
			}
		}
	}
	return replicators, nil
}
