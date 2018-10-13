package main

import (
    "encoding/csv"
    "fmt"
    "io"
    "log"
    "os"
    "strconv"
)

/*
  Theory of operation:
  1. We're not using a real database, after the RethinkDB debacle
  2. Operations which write to the DataStore are logged in the Journal and Flush()ed to disk
  3. The Journal can be Replay()ed, recreating the full state of the DataStore
  4. During playback, we shouldn't create new journal entries (duh)
*/
  
const OP_ADD_TEAM = "t"
const OP_ADD_TAG = "g"
const OP_INCREMENT_LAPS = "l"
const OP_ZERO_LAPS = "z"

const JOURNAL_NAME = "journal.csv"
const JOURNAL_MODE = 0666
const MAX_ENTRIES = 2

type Journal struct {
    entries []*string
    replaying bool
}


func (j *Journal) append(entry *string) {
    if !j.replaying {
        *entry += "\n"
        j.entries = append(j.entries, entry)
        if len(j.entries) > MAX_ENTRIES {
            j.Flush()
        }
    }
}
func (j *Journal) Team(name string, leader string) {
    entry := fmt.Sprintf(OP_ADD_TEAM + ",%v,%v", name, leader)
    j.append(&entry)
}
func (j *Journal) Tag(teamKey int, tagKey int) {
    entry := fmt.Sprintf(OP_ADD_TAG + ",%v,%v", teamKey, tagKey)
    j.append(&entry)
}
func (j *Journal) Lap(tagKey int) {
    entry := fmt.Sprintf(OP_INCREMENT_LAPS + ",%v", tagKey)
    j.append(&entry)
}
func (j *Journal) Zero() {
    entry := OP_ZERO_LAPS
    j.append(&entry)
}


func (j *Journal) Flush() error {
    if f, err := os.OpenFile(JOURNAL_NAME, os.O_CREATE|os.O_APPEND|os.O_WRONLY, JOURNAL_MODE); err == nil {
        for _, entry := range j.entries {
            f.WriteString(*entry)
        }
        f.Close()
    } else {
        log.Println("Flush OpenFile: ", err)
        return err
    }
    j.entries = nil
    return nil
}


func (j *Journal) Replay(ds* DataStore) error {
    if f, err := os.OpenFile(JOURNAL_NAME, os.O_RDONLY, JOURNAL_MODE); err == nil {
        j.replaying = true 
        reader := csv.NewReader(f)
        for {
            if record, err := reader.Read(); err == nil {
                if record[0] == OP_INCREMENT_LAPS {
                    tagKey, err := strconv.Atoi(record[1])
                    if err == nil {
                        ds.IncrementLaps(tagKey)
                    }
                } else if record[0] == OP_ADD_TAG {
                    teamKey, err1 := strconv.Atoi(record[1])
                    tagKey, err2 := strconv.Atoi(record[2])
                    if err1 == nil && err2 == nil {
                        ds.InsertTag(teamKey, tagKey)
                    }
                } else if record[0] == OP_ADD_TEAM {
                    ds.InsertTeam(record[1], record[2])
                } else if record[0] == OP_ZERO_LAPS {
                    ds.ZeroLaps()
                }
            } else if err == io.EOF {
                break
            } else {
                log.Println("Replay reader.Read: ", err)
                f.Close()
                j.replaying = false
                return err
            }
        }
        f.Close()
        j.replaying = false
    } else {
        log.Println("Replay OpenFile: ", err)
        return err
    }
    return nil
}

