package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/bvinc/go-sqlite-lite/sqlite3"
)

const (
	tTeams      = "teams"
	fTeamID     = "team_id"
	fTeamName   = "team_name"
	fTeamLeader = "team_leader"
	fTeamLaps   = "team_laps"

	tTags           = "tags"
	fTagID          = "tag_id"
	fTagLaps        = "tag_laps"
	fTagLastUpdated = "last_updated"

	minLapSecs = 2.0
)

// DataStore is the abstraction around a SQLite3 DB
type DataStore struct {
	conn *sqlite3.Conn
}

// Team is an in-memory representation of a row in the teams table
type Team struct {
	Laps   int
	Leader string
	Name   string
	TeamID int
}

// Teams is a list of Team structs
type Teams []*Team

// Tag is an in-memory representation of a row in the tags table
type Tag struct {
	TagID       int
	TeamID      int
	LastUpdated string
	Laps        int
}

// Tags is a list of Tag structs
type Tags []*Tag

// ConnectToDB is the way the web server connects to the DB from a goroutine
func ConnectToDB() *DataStore {
	conn, err := sqlite3.Open("relay.db")
	if err != nil {
		log.Printf("sqlite3.open %v", err)
	}
	ds := new(DataStore)
	ds.conn = conn

	err = ds.conn.Exec("PRAGMA foreign_keys = ON")
	if err != nil {
		log.Printf("PRAGMA %v", err)
	}

	s := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s(%s INTEGER NOT NULL PRIMARY KEY, %s TEXT,
        %s TEXT, %s INTEGER)`,
		tTeams, fTeamID, fTeamName, fTeamLeader, fTeamLaps)
	err = ds.conn.Exec(s)
	if err != nil {
		log.Printf("CREATE teams %v", err)
	}

	s = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s(%s INTEGER NOT NULL PRIMARY KEY, %s INTEGER,
        %s TEXT, %s INTEGER, FOREIGN KEY(%s) REFERENCES %s(%s))`,
		tTags, fTagID, fTeamID, fTagLastUpdated, fTagLaps, fTeamID, tTeams, fTeamID)
	err = ds.conn.Exec(s)
	if err != nil {
		log.Printf("CREATE tags %v", err)
	}

	// Allow indexed query for tags with a given team_id
	s = fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_team_id ON %s(%s)", tTags, fTeamID)
	err = ds.conn.Exec(s)
	if err != nil {
		log.Printf("CREATE idx_team_id %v", err)
	}
	return ds
}

// Close closes the SQLite3 conn
func (ds *DataStore) Close() {
	log.Println("DB closing")
	ds.conn.Close()
}

// IncrementLaps increments the lap counts
func (ds *DataStore) IncrementLaps(tagID int) error {
	// Get the row data for the tagID we got from the RFID reader
	s := fmt.Sprintf("SELECT %s,%s FROM %s WHERE %s = %d",
		fTeamID, fTagLastUpdated, tTags, fTagID, tagID)
	stmt, err := ds.conn.Prepare(s)
	if err != nil {
		log.Printf("Prepare IncrementLaps %v", err)
		return err
	}
	defer stmt.Close()
	var teamID int
	var lastUpdated string
	hasRow, err := stmt.Step()
	if err == nil && hasRow {
		err = stmt.Scan(&teamID, &lastUpdated)
		if err != nil {
			log.Printf("Scan IncrementLaps %v", err)
		}
	} else if !hasRow {
		log.Printf("Unassigned tag: %d", tagID)
		return nil
	}

	// Check for duplicate tag reads (or attempted cheating)
	now := time.Now()
	if then, err := time.Parse(time.RFC1123, lastUpdated); err == nil {
		if now.Sub(then).Seconds() < minLapSecs {
			log.Printf("Duplicate read: %d", tagID)
			return nil
		}
	}

	// Increment lap count and last updated in the tags table
	s = fmt.Sprintf("UPDATE %s SET %s = %s + 1, %s = \"%s\" WHERE %s = %d",
		tTags, fTagLaps, fTagLaps, fTagLastUpdated, now.Format(time.RFC1123), fTagID, tagID)
	if err = ds.conn.Exec(s); err != nil {
		log.Printf("Update tag laps %v", err)
		return err
	}

	// Increament lap count in teams table
	// I go back and forth over shadowing this data or calculating it
	s = fmt.Sprintf("UPDATE %s SET %s = %s + 1 WHERE %s = %d",
		tTeams, fTeamLaps, fTeamLaps, fTeamID, teamID)
	if err = ds.conn.Exec(s); err != nil {
		log.Printf("Update team laps %v", err)
		return err
	}
	return nil
}

func (ds *DataStore) getAllTeams(s string) (Teams, error) {
	var teams Teams
	stmt, err := ds.conn.Prepare(s)
	if err != nil {
		log.Printf("Prepare %v", err)
		return teams, err
	}
	defer stmt.Close()
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			log.Printf("Step %v", err)
			return teams, err
		}
		if !hasRow {
			break
		}

		var t Team
		err = stmt.Scan(&t.TeamID, &t.Name, &t.Leader, &t.Laps)
		if err != nil {
			log.Printf("Scan %v", err)
			return teams, err
		}
		teams = append(teams, &t)
	}
	return teams, err
}

// GetLeaderboard provides the list of N teams ordered by lap count
func (ds *DataStore) GetLeaderboard(maxSize int) (Teams, error) {
	s := fmt.Sprintf("SELECT * FROM %s ORDER BY %s DESC LIMIT %d", tTeams, fTeamLaps, maxSize)
	return ds.getAllTeams(s)
}

// GetTeams provides a list of teams
func (ds *DataStore) GetTeams() (Teams, error) {
	s := fmt.Sprintf("SELECT * FROM %s", tTeams)
	return ds.getAllTeams(s)
}

// GetTeamName is a helper function for the "/teams/" handler
func (ds *DataStore) GetTeamName(teamID int) (string, error) {
	s := fmt.Sprintf("SELECT %s FROM %s WHERE %s = %d", fTeamName, tTeams, fTeamID, teamID)
	stmt, err := ds.conn.Prepare(s)
	defer stmt.Close()
	if err != nil {
		return "", err
	}

	hasRow, err := stmt.Step()
	if err != nil {
		return "", err
	}

	var name string
	if hasRow {
		err = stmt.Scan(&name)
		if err != nil {
			return "", err
		}
	}
	return name, nil
}

func (ds *DataStore) insertTeams(teams Teams) error {
	s := fmt.Sprintf("INSERT INTO %s(%s, %s, %s, %s) VALUES(?, ?, ?, ?)",
		tTeams, fTeamID, fTeamName, fTeamLeader, fTeamLaps)
	stmt, err := ds.conn.Prepare(s)
	if err != nil {
		log.Printf("Prepare insertTeams %v", err)
		return err
	}
	defer stmt.Close()

	for _, t := range teams {
		if err = stmt.Exec(t.TeamID, t.Name, t.Leader, 0); err != nil {
			log.Printf("Exec insertTeams %v", err)
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
	if err != nil {
		log.Printf("InsertTeams %v", err)
	}
	return err
}

func (ds *DataStore) insertTags(tags Tags) error {
	s := fmt.Sprintf("INSERT INTO %s(%s, %s, %s, %s) VALUES(?, ?, ?, ?)",
		tTags, fTagID, fTeamID, fTagLastUpdated, fTagLaps)
	stmt, err := ds.conn.Prepare(s)
	if err != nil {
		log.Printf("Prepare insertTags %v", err)
		return err
	}
	defer stmt.Close()
	for _, tag := range tags {
		if err = stmt.Exec(tag.TagID, tag.TeamID, "", 0); err != nil {
			log.Printf("Exec insertTags %v", err)
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
	if err != nil {
		log.Printf("InsertTags: %v", err)
	}
	return err
}

// GetTagsForTeam supports the "/team/?" handler
func (ds *DataStore) GetTagsForTeam(teamID int) (Tags, error) {
	s := fmt.Sprintf("SELECT * FROM %s WHERE %s = %d", tTags, fTeamID, teamID)
	var tags Tags
	stmt, err := ds.conn.Prepare(s)
	if err != nil {
		log.Printf("GetTagsForTeam Prepare %v", err)
		return tags, err
	}
	defer stmt.Close()
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			log.Printf("GetTagsForTeam Step %v", err)
			return tags, err
		}
		if !hasRow {
			break
		}

		var tag Tag
		err = stmt.Scan(&tag.TagID, &tag.TeamID, &tag.LastUpdated, &tag.Laps)
		if err != nil {
			log.Printf("GetTagsForTeam Scan %v", err)
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
