package main

import (
	"encoding/csv"
	"errors"
	"io"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/sqlite"
)

const minLapSecs = 60.0

type (
	// DataStore is the abstraction around a SQLite3 DB
	DataStore struct {
		conn *gorm.DB
	}

	// Team is an in-memory representation of a row in the teams table
	Team struct {
		ID     int
		Laps   int
		Name   string
		Leader string
		Hours  uint   // bitfield where each bit represents the team being on track for one of the 24 hours of the event
		Rank   string `gorm:"-"` // transient for /teams/ - not in DB
	}

	// Teams is an array of Team structs
	Teams []*Team

	// Tag is an in-memory representation of a row in the tags table
	Tag struct {
		ID      int
		Laps    int
		TeamID  int `gorm:"index:team_id"`
		Updated int64
	}

	// Tags is an array of Tag structs
	Tags []*Tag

	// Notification is how the server backend tells a browser client to display a tag read
	Notification struct {
		TagID    int
		TeamID   int
		TeamLaps int
		TeamName string
		TeamRank string
	}
)

var (
	// ErrDuplicateRead means TagID was read multiple times within minLapSecs.
	// My RFID clients do deduplication, so this could mean the tag was read by both readers,
	// or it could mean someone is standing in front of an antenna
	ErrDuplicateRead = errors.New("duplicate read")

	// ErrUnassignedTag means TagID was not found in the tags table
	ErrUnassignedTag = errors.New("unassigned tag")

	initialized = false
)

// ConnectToDB is the way the web server connects to the DB from a goroutine
func ConnectToDB() (*DataStore, error) {
	conn, err := gorm.Open("sqlite3", "lapcounter.db")
	if err != nil {
		log.Fatalln("Open: ", err)
		return nil, err
	}
	ds := new(DataStore)
	ds.conn = conn
	if !initialized {
		ds.conn.AutoMigrate(&Team{}, &Tag{})
		initialized = true
		log.Println("database ready")
	}
	return ds, nil
}

// Close closes the SQLite3 conn
func (ds *DataStore) Close() {
	ds.conn.Close()
}

// IncrementLaps updates the DB and generates notifications for the browser client(s)
func (ds *DataStore) IncrementLaps(tagID int, hour uint) (Notification, error) {
	var notif Notification
	var tag Tag
	if ds.conn.First(&tag, tagID).RecordNotFound() {
		return notif, ErrUnassignedTag
	}

	// Check for duplicate tag reads (or attempted cheating)
	now := time.Now()
	then := time.Unix(0, tag.Updated)
	if now.Sub(then).Seconds() < minLapSecs {
		return notif, ErrDuplicateRead
	}

	// Update the tag's row
	tag.Laps++
	tag.Updated = now.UnixNano()
	if err := ds.conn.Model(&tag).Update(&tag).Error; err != nil {
		return notif, err
	}

	// Update the team's row
	var team Team
	err := ds.conn.First(&team, tag.TeamID).Error
	if err != nil {
		return notif, err
	}
	team.Laps++
	team.Hours |= uint(1) << hour
	err = ds.conn.Model(&team).Update(&team).Error
	if err != nil {
		return notif, err
	}

	notif = Notification{
		TagID:    tagID,
		TeamID:   team.ID,
		TeamLaps: team.Laps,
		TeamName: team.Name,
		TeamRank: ds.getOneTeamRank(tag.TeamID),
	}
	return notif, nil
}

// GetTeamRanks builds a map of teamIDs to string, e.g. "1st" or "23rd (T)" for ties
func (ds *DataStore) getTeamRanks(teams Teams) (map[int]string, error) {
	ranks := make(map[int]string)
	rank, nextRank := 0, 0
	prevLaps := math.MaxInt32
	for _, team := range teams {
		nextRank++
		if team.Laps < prevLaps {
			rank = nextRank
		}
		ranks[team.ID] = strconv.Itoa(rank)
		if team.Laps == prevLaps {
			ranks[team.ID] += " (T)"
		}
		prevLaps = team.Laps
	}
	return ranks, nil
}

func (ds *DataStore) getOneTeamRank(teamID int) string {
	var teams Teams
	err := ds.conn.Order("laps desc").Find(&teams).Error
	if err != nil {
		return ""
	}
	ranks, err := ds.getTeamRanks(teams)
	if err != nil {
		return ""
	}
	return ranks[teamID]
}

// GetOneTeam is a helper function for the /team/? handler
func (ds *DataStore) GetOneTeam(teamID int) (Team, error) {
	var team Team
	err := ds.conn.First(&team, teamID).Error
	return team, err
}

// GetTeams is a helper function for the /teams/ handler
func (ds *DataStore) GetTeams(key string, order string) (Teams, error) {
	if key == "" { // Convenience so I don't have to type sort/order in the URL
		key = "id"
	}
	if order == "" {
		order = "ASC"
	}
	orderParam := key + " " + order

	var teams Teams
	err := ds.conn.Order(orderParam).Find(&teams).Error
	if err != nil {
		return teams, err
	}

	ranks, err := ds.getTeamRanks(teams)
	if err != nil {
		return teams, err
	}
	for _, team := range teams {
		team.Rank = ranks[team.ID]
	}

	return teams, nil
}

func (ds *DataStore) insertTeam(team *Team) error {
	team.Laps = 0
	team.Hours = 0
	err := ds.conn.Create(team).Error
	if err != nil {
		return err
	}
	return nil
}

// InsertTag inserts a tag into the database
func (ds *DataStore) InsertTag(tag Tag) error {
	tag.Laps = 0
	tag.Updated = 0
	err := ds.conn.Create(&tag).Error
	if err != nil {
		return err
	}
	return nil
}

// GetTagsForTeam supports the "/team/?" handler
func (ds *DataStore) GetTagsForTeam(teamID int) (Tags, error) {
	var tags Tags
	err := ds.conn.Where("team_id = ?", teamID).Find(&tags).Error
	return tags, err
}

// Import reads the specified CSV file and populates the DB with teams and optional tags
func (ds *DataStore) Import(fname string) error {
	numTeams, numTags := 0, 0
	if file, err := os.Open(fname); err == nil {
		defer file.Close()
		reader := csv.NewReader(file)
		reader.Read() // Skip header row
		for {
			record, err := reader.Read()
			if err == io.EOF {
				log.Printf("imported %d teams and %d tags from %s", numTeams, numTags, fname)
				break
			}
			if err != nil {
				return err
			}
			team := Team{Name: record[0], Leader: record[1]}
			err = ds.insertTeam(&team)
			if err != nil {
				return err
			}
			numTeams++

			tagRange := record[4]
			if tagRange != "" { // Not everyone gets tags ahead of time
				splitTags := strings.Split(tagRange, "-")
				firstTag, _ := strconv.Atoi(splitTags[0])
				lastTag, _ := strconv.Atoi(splitTags[1])
				for tagID := firstTag; tagID <= lastTag; tagID++ {
					err := ds.InsertTag(Tag{ID: tagID, TeamID: team.ID})
					if err != nil {
						return err
					}
					numTags++
				}
			}
		}
	} else {
		return err
	}
	return nil
}
