package main

import (
	"encoding/csv"
	"errors"
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
	fTeamLaps   = "team_laps"
	fTeamLeader = "team_leader"
	fTeamName   = "team_name"
	fTeamID     = "team_id"

	tTags           = "tags"
	fTagID          = "tag_id"
	fTagLaps        = "tag_laps"
	fTagLastUpdated = "tag_last_updated"

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
	Rank   int // transient - not in DB
	TeamID int
}

// Teams is an array of Team structs
type Teams []*Team

// Tag is an in-memory representation of a row in the tags table
type Tag struct {
	Laps        int
	LastUpdated string
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
}

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

	s := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s(%s INTEGER NOT NULL PRIMARY KEY, %s INTEGER,
        %s TEXT, %s TEXT)`,
		tTeams, fTeamID, fTeamLaps, fTeamLeader, fTeamName)
	err = ds.conn.Exec(s)
	if err != nil {
		log.Printf("CREATE teams %v", err)
	}

	s = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s(%s INTEGER NOT NULL PRIMARY KEY, %s INTEGER,
        %s TEXT, %s INTEGER, FOREIGN KEY(%s) REFERENCES %s(%s))`,
		tTags, fTagID, fTagLaps, fTagLastUpdated, fTeamID, fTeamID, tTeams, fTeamID)
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

func (ds *DataStore) getOneTag(tagID int, pLaps *int, pLastUpdated *string, pTagID *int, pTeamID *int) error {
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

func (ds *DataStore) incrementLaps(pTag *Tag) error {
	// Check for duplicate tag reads (or attempted cheating)
	now := time.Now()
	if then, err := time.Parse(time.RFC1123, pTag.LastUpdated); err == nil {
		if now.Sub(then).Seconds() < minLapSecs {
			return nil
		}
	}

	// Increment lap count and last updated in the tags table
	s := fmt.Sprintf("UPDATE %s SET %s = %s + 1, %s = \"%s\" WHERE %s = %d",
		tTags, fTagLaps, fTagLaps, fTagLastUpdated, now.Format(time.RFC1123), fTagID, pTag.TagID)
	if err := ds.conn.Exec(s); err != nil {
		return err
	}

	// Increament lap count in teams table
	// I go back and forth over shadowing this data or calculating it
	s = fmt.Sprintf("UPDATE %s SET %s = %s + 1 WHERE %s = %d",
		tTeams, fTeamLaps, fTeamLaps, fTeamID, pTag.TeamID)
	if err := ds.conn.Exec(s); err != nil {
		return err
	}
	return nil
}

// IncrementLaps updates the DB and generates notifications for the browser client(s)
func (ds *DataStore) IncrementLaps(tagID int) (Notification, error) {
	var tag Tag
	var notif Notification

	if err := ds.getOneTag(tagID, &tag.Laps, &tag.LastUpdated, &tag.TagID, &tag.TeamID); err != nil {
		return notif, err
	}
	if err := ds.incrementLaps(&tag); err != nil {
		return notif, err
	}
	var leader string // unused
	if err := ds.getOneTeam(tag.TeamID, &notif.TeamLaps, &leader, &notif.TeamName, &notif.TeamID); err != nil {
		return notif, err
	}
	notif.TagID = tagID
	return notif, nil
}

// GetOneTeam returns all fields for teamID's row in the teams table
func (ds *DataStore) getOneTeam(teamID int, pLaps *int, pLeader *string, pName *string, pTeamID *int) error {
	s := fmt.Sprintf("SELECT %s, %s, %s, %s FROM %s WHERE %s = %d",
		fTeamID, fTeamLaps, fTeamLeader, fTeamName, tTeams, fTeamID, teamID)
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
		if err := stmt.Scan(pTeamID, pLaps, pLeader, pName); err != nil {
			return err
		}
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
		err = stmt.Scan(&t.TeamID, &t.Laps, &t.Leader, &t.Name)
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
	s := fmt.Sprintf("SELECT %s, %s, %s, %s FROM %s ORDER BY %s DESC LIMIT %d",
		fTeamID, fTeamLaps, fTeamLeader, fTeamName, tTeams, fTeamLaps, maxSize)
	return ds.getAllTeams(s)
}

//GetOneTeam is a helper function for the /teams/ handler
func (ds *DataStore) GetOneTeam(teamID int) (Team, error) {
	var team Team
	err := ds.getOneTeam(teamID, &team.Laps, &team.Leader, &team.Name, &team.TeamID)
	return team, err
}

// GetTeams provides a list of teams
func (ds *DataStore) GetTeams() (Teams, error) {
	s := fmt.Sprintf("SELECT %s, %s, %s, %s FROM %s", fTeamID, fTeamLaps, fTeamLeader, fTeamName, tTeams)
	return ds.getAllTeams(s)
}

func (ds *DataStore) insertTeams(teams Teams) error {
	s := fmt.Sprintf("INSERT INTO %s(%s, %s, %s, %s) VALUES(?, ?, ?, ?)",
		tTeams, fTeamLaps, fTeamLeader, fTeamName, fTeamID)
	stmt, err := ds.conn.Prepare(s)
	if err != nil {
		log.Printf("Prepare insertTeams %v", err)
		return err
	}
	defer stmt.Close()

	for _, t := range teams {
		if err = stmt.Exec(0, t.Leader, t.Name, t.TeamID); err != nil {
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
	s := fmt.Sprintf("SELECT %s, %s, %s, %s FROM %s WHERE %s = %d",
		fTagID, fTeamID, fTagLastUpdated, fTagLaps, tTags, fTeamID, teamID)
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
