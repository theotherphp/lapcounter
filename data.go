package main

import (
    "log"
    "time"
)

/* 
    Theory of Operation
    1. It's basically an in-memory DataStore, backed by the persistent Journal
    2. Tags are stored in a map, where the key is the integer number encoded on the tag
    3. Teams are stores in a slice, by value. The index in the slice is used as a key, which
       is legit because teams are never removed
*/

type Team struct {
    Name string
    Leader string
    Laps int
    TagKeys []int
}

type Tag struct {
    Laps int
    LastUpdated time.Time
    TagKey int
    TeamKey int
}

type DataStore struct {
    teams []*Team
    tags map[int]*Tag
    journal *Journal
}

const MIN_LAP_SECS = 10

func InitDataStore() *DataStore {
    ds := new(DataStore)
    ds.journal = new(Journal)

    idx, _ := ds.InsertTeam("LYB", "JB")
    ds.tags = make(map[int]*Tag)
    ds.InsertTag(idx, 0)

    return ds
}


func (ds *DataStore) InsertTeam(name string, leader string) (int, error) {
    team := Team{Name:name, Leader:leader}
    ds.teams = append(ds.teams, &team)
    ds.journal.Team(name, leader)
    return len(ds.teams) - 1, nil
}


func (ds *DataStore) GetTeams() ([]*Team) {
    return ds.teams
}


func (ds *DataStore) GetTeam(teamKey int) *Team {
    return ds.teams[teamKey]
}


func (ds *DataStore) InsertTag(teamKey int, tagKey int) error {
    if _, exists := ds.tags[tagKey]; exists {
        log.Println("already registered: ", tagKey)
        return nil
    } else {
        ds.tags[tagKey] = &Tag {
            TagKey: tagKey,
            TeamKey: teamKey, 
            LastUpdated: time.Now(),
        }
        ds.teams[teamKey].TagKeys = append(ds.teams[teamKey].TagKeys, tagKey)
        ds.journal.Tag(teamKey, tagKey)
    }
    return nil
}


func (ds *DataStore) GetTagIDs() ([]int) {
    tagKeys := []int{}
    for tagKey, _ := range ds.tags {
        tagKeys = append(tagKeys, tagKey)
    }
    return tagKeys
}


func (ds *DataStore) GetTagsByTeam(teamKey int) []*Tag {
    matchingTags := make([]*Tag,0)
    for tagKey := range ds.teams[teamKey].TagKeys {
        matchingTags = append(matchingTags, ds.tags[tagKey])
    }
    return matchingTags
}


func (ds *DataStore) IncrementLaps(tagKey int) {
    if _, exists := ds.tags[tagKey]; exists {
        tag := ds.tags[tagKey]
        timeDelta := time.Since(tag.LastUpdated).Seconds()
        if  timeDelta > MIN_LAP_SECS {
            tag.Laps++
            tag.LastUpdated = time.Now()
            log.Printf("tag: %v laps: %v\n", tagKey, tag.Laps)

            team := ds.teams[tag.TeamKey]
            team.Laps++

            ds.journal.Lap(tagKey)
        } else {
            log.Printf("too soon: tag %d last read %.1f secs ago (min %d secs)\n", tagKey, timeDelta, MIN_LAP_SECS)
        }
    } else {
        log.Println("unregistered tag: ", tagKey)
    }
}


func (ds *DataStore) ZeroLaps() {
    for _, tag := range ds.tags {
        tag.Laps = 0
    }
    for idx := range ds.teams {
        ds.teams[idx].Laps = 0
    }
    ds.journal.Zero()
}


func (ds *DataStore) Close() error {
    err := ds.journal.Flush()
    return err
}
