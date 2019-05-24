package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/bvinc/go-sqlite-lite/sqlite3"
)

const (
	tTeams      = "teams"
	fTeamHours  = "team_hours"
	fTeamLaps   = "team_laps"
	fTeamLeader = "team_leader"
	fTeamName   = "team_name"
	fTeamID     = "team_id"

	tTags           = "tags"
	fTagID          = "tag_id"
	fTagLaps        = "tag_laps"
	fTagLastUpdated = "tag_last_updated"

	minLapSecs = 60.0
)

// DataStore is the abstraction around a SQLite3 DB
type DataStore struct {
	conn *sqlite3.Conn
}

// Team is an in-memory representation of a row in the teams table
type Team struct {
	Hours  uint // bitfield where each bit represents the team being on track for one of the 24 hours of the event
	Laps   int
	Leader string
	Name   string
	Rank   string // transient - not in DB
	TeamID int
}

// Teams is an array of Team structs
type Teams []*Team

// Tag is an in-memory representation of a row in the tags table
type Tag struct {
	Laps        int
	LastUpdated int64
	TagID       int
	TeamID      int
}

// Tags is an array of Tag structs
type Tags []*Tag

// Notification is how the server backend tells a browser client to display a tag read
type Notification struct {
	TagID    int
	TeamID   int
	TeamLaps int
	TeamName string
	TeamRank string
}

// ErrDuplicateRead is a soft error generated when a tag is received multiple times within minLapSecs
var ErrDuplicateRead = errors.New("Duplicate read")
var initialized = false

// ConnectToDB is the way the web server connects to the DB from a goroutine
func ConnectToDB() (*DataStore, error) {
	conn, err := sqlite3.Open("relay.db")
	if err != nil {
		log.Fatalln("Open: ", err)
		return nil, err
	}
	ds := new(DataStore)
	ds.conn = conn
	// Wait at most one second, potentially across multiple attempts
	// https://www.sqlite.org/c3ref/busy_timeout.html
	ds.conn.BusyTimeout(time.Second)

	if !initialized {
		// TODO: why does .dump say foreign_keys = off
		if err := ds.conn.Exec("PRAGMA foreign_keys = ON"); err != nil {
			log.Fatalln("Pragma: ", err)
			return nil, err
		}

		s := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s(%s INTEGER NOT NULL PRIMARY KEY, %s INTEGER,
			%s TEXT, %s TEXT, %s INTEGER)`,
			tTeams, fTeamID, fTeamLaps, fTeamLeader, fTeamName, fTeamHours)
		if err := ds.conn.Exec(s); err != nil {
			log.Fatalln("CREATE teams: ", err)
			return nil, err
		}

		s = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s(%s INTEGER NOT NULL PRIMARY KEY, %s INTEGER,
			%s TEXT, %s INTEGER, FOREIGN KEY(%s) REFERENCES %s(%s))`,
			tTags, fTagID, fTagLaps, fTagLastUpdated, fTeamID, fTeamID, tTeams, fTeamID)
		if err := ds.conn.Exec(s); err != nil {
			log.Fatalln("CREATE tags: ", err)
			return nil, err
		}

		// Allow indexed query for tags with a given team_id
		s = fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_team_id ON %s(%s)", tTags, fTeamID)
		if err := ds.conn.Exec(s); err != nil {
			log.Fatalln("CREATE idx_team_id: ", err)
			return nil, err
		}
		initialized = true
		log.Println("initialized DB")
	}
	return ds, nil
}

// Close closes the SQLite3 conn
func (ds *DataStore) Close() {
	ds.conn.Close()
}

func (ds *DataStore) incrementHours(teamID int, hours uint) error {
	s := fmt.Sprintf("UPDATE %s SET %s = %d WHERE %s = %d",
		tTeams, fTeamHours, hours, fTeamID, teamID)
	if err := ds.conn.Exec(s); err != nil {
		log.Println("err: ", err)
		return err
	}
	return nil
}

func (ds *DataStore) incrementLaps(pTag *Tag) error {
	// Check for duplicate tag reads (or attempted cheating)
	now := time.Now()
	then := time.Unix(0, pTag.LastUpdated)
	if now.Sub(then).Seconds() < minLapSecs {
		return ErrDuplicateRead
	}

	// Increment lap count and last updated in the tags table
	s := fmt.Sprintf("UPDATE %s SET %s = %s + 1, %s = %d WHERE %s = %d",
		tTags, fTagLaps, fTagLaps, fTagLastUpdated, now.UnixNano(), fTagID, pTag.TagID)
	if err := ds.conn.Exec(s); err != nil {
		return err
	}

	// Increament lap count in teams table
	s = fmt.Sprintf("UPDATE %s SET %s = %s + 1 WHERE %s = %d",
		tTeams, fTeamLaps, fTeamLaps, fTeamID, pTag.TeamID)
	if err := ds.conn.Exec(s); err != nil {
		return err
	}
	return nil
}

// IncrementLaps updates the DB and generates notifications for the browser client(s)
func (ds *DataStore) IncrementLaps(tagID int, hour uint) (Notification, error) {
	var tag Tag
	var notif Notification

	if err := ds.getOneTag(tagID, &tag.Laps, &tag.LastUpdated, &tag.TagID, &tag.TeamID); err != nil {
		return notif, err
	}
	if err := ds.incrementLaps(&tag); err != nil {
		return notif, err
	}
	var leader string // unused
	var hours uint
	if err := ds.getOneTeam(tag.TeamID, &hours, &notif.TeamLaps, &leader, &notif.TeamName, &notif.TeamID); err != nil {
		return notif, err
	}

	hourBit := uint(1) << hour
	if hours&hourBit == 0 {
		err := ds.incrementHours(tag.TeamID, hours|hourBit)
		if err != nil {
			return notif, err
		}
	}

	notif.TagID = tagID
	notif.TeamRank = ds.getOneTeamRank(tag.TeamID)
	return notif, nil
}

// getAllTeams is a helper function shared between several API functions
func (ds *DataStore) getAllTeams(s string) (Teams, error) {
	var teams Teams
	stmt, err := ds.conn.Prepare(s)
	if err != nil {
		return teams, err
	}
	defer stmt.Close()
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return teams, err
		}
		if !hasRow {
			break
		}

		var t Team
		var hours int
		err = stmt.Scan(&t.TeamID, &hours, &t.Laps, &t.Leader, &t.Name)
		if err != nil {
			return teams, err
		}
		t.Hours = uint(hours) // uint is unscannable to SQLite
		teams = append(teams, &t)
	}
	return teams, err
}

// GetLeaderboard provides the list of N teams ordered by lap count
func (ds *DataStore) GetLeaderboard(maxSize int) (Teams, error) {
	s := fmt.Sprintf("SELECT %s, %s, %s, %s, %s FROM %s ORDER BY %s DESC LIMIT %d",
		fTeamID, fTeamHours, fTeamLaps, fTeamLeader, fTeamName, tTeams, fTeamLaps, maxSize)
	return ds.getAllTeams(s)
}

// GetTeamRanks builds a map of teamIDs to string, e.g. "1st" or "23rd (T)" for ties
func (ds *DataStore) getTeamRanks() (map[int]string, error) {
	ranks := make(map[int]string)
	teams, err := ds.GetLeaderboard(999)
	if err != nil {
		log.Println("GetLeaderboard: ", err)
		return ranks, err
	}
	rank, nextRank := 0, 0
	prevLaps := math.MaxInt32
	for _, t := range teams {
		nextRank++
		if t.Laps < prevLaps {
			rank = nextRank
		}
		ranks[t.TeamID] = strconv.Itoa(rank)
		if t.Laps == prevLaps {
			ranks[t.TeamID] += " (T)"
		}
		prevLaps = t.Laps
	}
	return ranks, nil
}

func (ds *DataStore) getOneTeamRank(teamID int) string {
	ranks, err := ds.getTeamRanks()
	if err != nil {
		return ""
	}
	return ranks[teamID]
}

func (ds *DataStore) getOneTeam(teamID int, pHours *uint, pLaps *int, pLeader *string, pName *string, pTeamID *int) error {
	s := fmt.Sprintf("SELECT %s, %s, %s, %s, %s FROM %s WHERE %s = %d",
		fTeamID, fTeamHours, fTeamLaps, fTeamLeader, fTeamName, tTeams, fTeamID, teamID)
	stmt, err := ds.conn.Prepare(s)
	if err != nil {
		return err
	}
	defer stmt.Close()
	hasRow, err := stmt.Step()
	if err != nil {
		return err
	}
	if hasRow {
		var hours int
		if err := stmt.Scan(pTeamID, &hours, pLaps, pLeader, pName); err != nil {
			return err
		}
		*pHours = uint(hours) // uint is unscannable to SQLite
	}
	return nil
}

// GetOneTeam is a helper function for the /team/? handler
func (ds *DataStore) GetOneTeam(teamID int) (Team, error) {
	var team Team
	err := ds.getOneTeam(teamID, &team.Hours, &team.Laps, &team.Leader, &team.Name, &team.TeamID)
	return team, err
}

// GetTeams is a helper function for the /teams/ handler
func (ds *DataStore) GetTeams(key string, order string) (Teams, error) {
	if key == "" { // Convenience so I don't have to type sort/order in the URL
		key = "team_id"
	}
	if order == "" {
		order = "ASC"
	}
	s := fmt.Sprintf("SELECT %s, %s, %s, %s, %s FROM %s ORDER BY %s %s",
		fTeamID, fTeamHours, fTeamLaps, fTeamLeader, fTeamName, tTeams, key, order)
	teams, err := ds.getAllTeams(s)
	if err != nil {
		return teams, err
	}
	ranks, err := ds.getTeamRanks()
	if err != nil {
		return teams, err
	}
	for _, t := range teams {
		t.Rank = ranks[t.TeamID]
	}
	return teams, nil
}

func (ds *DataStore) insertTeams(teams Teams) error {
	s := fmt.Sprintf("INSERT INTO %s(%s, %s, %s, %s, %s) VALUES(?, ?, ?, ?, ?)",
		tTeams, fTeamLaps, fTeamLeader, fTeamName, fTeamID, fTeamHours)
	stmt, err := ds.conn.Prepare(s)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, t := range teams {
		laps := 0
		hours := 0
		if err = stmt.Exec(laps, t.Leader, t.Name, t.TeamID, hours); err != nil {
			return err
		}
	}

	return nil
}

// InsertTeams takes a list of Team structs and inserts them in the DB
func (ds *DataStore) InsertTeams(teams Teams) error {
	err := ds.conn.WithTx(func() error {
		return ds.insertTeams(teams)
	})
	return err
}

func (ds *DataStore) insertTags(tags Tags) error {
	s := fmt.Sprintf("INSERT INTO %s(%s, %s, %s, %s) VALUES(?, ?, ?, ?)",
		tTags, fTagID, fTeamID, fTagLastUpdated, fTagLaps)
	stmt, err := ds.conn.Prepare(s)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, tag := range tags {
		if err := stmt.Exec(tag.TagID, tag.TeamID, 0, 0); err != nil {
			return err
		}
	}
	return nil
}

// InsertTags takes a list of Tag structs and inserts them in the DB
func (ds *DataStore) InsertTags(tags Tags) error {
	err := ds.conn.WithTx(func() error {
		return ds.insertTags(tags)
	})
	return err
}

func (ds *DataStore) getOneTag(tagID int, pLaps *int, pLastUpdated *int64, pTagID *int, pTeamID *int) error {
	s := fmt.Sprintf("SELECT %s, %s, %s, %s FROM %s WHERE %s = %d",
		fTagID, fTagLaps, fTagLastUpdated, fTeamID, tTags, fTagID, tagID)
	stmt, err := ds.conn.Prepare(s)
	if err != nil {
		return err
	}
	defer stmt.Close()
	hasRow, err := stmt.Step()
	if err == nil && hasRow {
		if err := stmt.Scan(pTagID, pLaps, pLastUpdated, pTeamID); err != nil {
			return err
		}
	} else if !hasRow {
		return errors.New("Unassigned tag")
	}
	return nil
}

// GetTagsForTeam supports the "/team/?" handler
func (ds *DataStore) GetTagsForTeam(teamID int) (Tags, error) {
	s := fmt.Sprintf("SELECT %s, %s, %s, %s FROM %s WHERE %s = %d",
		fTagID, fTeamID, fTagLastUpdated, fTagLaps, tTags, fTeamID, teamID)
	var tags Tags
	stmt, err := ds.conn.Prepare(s)
	if err != nil {
		return tags, err
	}
	defer stmt.Close()
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return tags, err
		}
		if !hasRow {
			break
		}

		var tag Tag
		err = stmt.Scan(&tag.TagID, &tag.TeamID, &tag.LastUpdated, &tag.Laps)
		if err != nil {
			return tags, err
		}
		tags = append(tags, &tag)
	}
	return tags, err
}

// Import reads the specified CSV file and populates the DB with teams and optional tags
func (ds *DataStore) Import(fname string) error {
	if file, err := os.Open(fname); err == nil {
		defer file.Close()
		reader := csv.NewReader(file)
		reader.Read() // Skip header row
		teamID := 0   // TeamID is just insertion order
		var teams Teams
		var tags Tags
		for {
			record, err := reader.Read()
			if err == io.EOF {
				if err = ds.InsertTeams(teams); err != nil {
					return err
				}
				if err = ds.InsertTags(tags); err != nil {
					return err
				}
				log.Printf("imported %d teams and %d tags from %s", len(teams), len(tags), fname)
				break
			}
			if err != nil {
				return err
			}
			teams = append(teams, &Team{TeamID: teamID, Name: record[0], Leader: record[1]})
			tagRange := record[5]
			if tagRange != "" { // Not everyone gets tags ahead of time
				splitTags := strings.Split(tagRange, "-")
				firstTag, _ := strconv.Atoi(splitTags[0])
				lastTag, _ := strconv.Atoi(splitTags[1])
				for tagID := firstTag; tagID <= lastTag; tagID++ {
					tags = append(tags, &Tag{TagID: tagID, TeamID: teamID})
				}
			}
			teamID++
		}
	} else {
		return err
	}
	return nil
}
