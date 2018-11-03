package main 

import (
    "fmt"
    "log"
    "time"
    "github.com/bvinc/go-sqlite-lite/sqlite3"
)

const (
    TEAMS_TBL   = "teams"
    TEAM_ID     = "team_id"
    TEAM_NAME   = "team_name"
    TEAM_LEADER = "team_leader"
    TEAM_LAPS   = "team_laps"

    TAGS_TBL = "tags"
    TAG_ID   = "tag_id"
    TAG_LAPS = "tag_laps"
    TAG_TIME = "tag_time"

    MIN_LAP_SECS = 2.0
)

type DataStore struct {
    conn *sqlite3.Conn
}

type Team struct {
    Laps int
    Leader string
    Name string
    TeamID int
}
type Teams []*Team

type Tag struct {
    TagID int
    TeamID int
    LastUpdated string
    Laps int
}
type Tags []*Tag


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
        TEAMS_TBL, TEAM_ID, TEAM_NAME, TEAM_LEADER, TEAM_LAPS)
    err = ds.conn.Exec(s)
    if err != nil {
        log.Printf("CREATE teams %v", err)
    }

    s = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s(%s INTEGER NOT NULL PRIMARY KEY, %s INTEGER,
        %s TEXT, %s INTEGER, FOREIGN KEY(%s) REFERENCES %s(%s))`, 
        TAGS_TBL, TAG_ID, TEAM_ID, TAG_TIME, TAG_LAPS, TEAM_ID, TEAMS_TBL, TEAM_ID)
    err = ds.conn.Exec(s)
    if err != nil {
        log.Printf("CREATE tags %v", err)
    }

    // Allow indexed query for tags with a given team_id
    s = fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_team_id ON %s(%s)", TAGS_TBL, TEAM_ID)
    err = ds.conn.Exec(s)
    if err != nil {
        log.Printf("CREATE idx_team_id %v", err)
    }
    return ds
}

func (ds *DataStore) Close() {
    ds.conn.Close()
}



func (ds *DataStore) IncrementLaps(tag_id int) error {
    // Get the row data for the tag_id we got from the RFID reader
    s := fmt.Sprintf("SELECT %s,%s FROM %s WHERE %s = %d", 
        TEAM_ID, TAG_TIME, TAGS_TBL, TAG_ID, tag_id)
    stmt, err := ds.conn.Prepare(s)
    if err != nil {
        log.Printf("Prepare IncrementLaps %v", err)
        return err
    }
    defer stmt.Close()
    var team_id int
    var last_updated string
    hasRow, err := stmt.Step()
    if err == nil && hasRow {
        err = stmt.Scan(&team_id, &last_updated)
        if err != nil {
            log.Printf("Scan IncrementLaps %v", err)
        }
    }

    // Check for duplicate tag reads (or attempted cheating)    
    now := time.Now()
    if then, err := time.Parse(time.RFC1123, last_updated); err == nil {
        if now.Sub(then).Seconds() < MIN_LAP_SECS {
            log.Printf("Duplicate read: %d", tag_id)
            return nil
        }
    }

    // Increment lap count and last updated in the tags table
    s = fmt.Sprintf("UPDATE %s SET %s = %s + 1, %s = \"%s\" WHERE %s = %d", 
        TAGS_TBL, TAG_LAPS, TAG_LAPS, TAG_TIME, now.Format(time.RFC1123), TAG_ID, tag_id)
    if err = ds.conn.Exec(s); err != nil {
        log.Printf("Update tag laps %v", err)
        return err
    }

    // Increament lap count in teams table
    // I go back and forth over shadowing this data or calculating it 
    s = fmt.Sprintf("UPDATE %s SET %s = %s + 1 WHERE %s = %d", 
        TEAMS_TBL, TEAM_LAPS, TEAM_LAPS, TEAM_ID, team_id)
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

        var team_id, laps int
        var team_name, team_leader string
        err = stmt.Scan(&team_id, &team_name, &team_leader, &laps)
        if err != nil {
            log.Printf("Scan %v", err)
            return teams, err
        }
        teams = append(teams, &Team{TeamID: team_id, Name: team_name, Leader: team_leader, Laps: laps})
    }
    return teams, err
}

func (ds *DataStore) GetLeaderboard(maxSize int) (Teams, error) {
    s := fmt.Sprintf("SELECT * FROM %s ORDER BY %s DESC LIMIT %d", TEAMS_TBL, TEAM_LAPS, maxSize)
    return ds.getAllTeams(s)
}

func (ds *DataStore) GetTeams() (Teams, error) {
    s := fmt.Sprintf("SELECT * FROM %s", TEAMS_TBL)
    return ds.getAllTeams(s)
}

func (ds *DataStore) GetTeamName(team_id int) (string, error) {
    s:= fmt.Sprintf("SELECT %s FROM %s WHERE %s = %d", TEAM_NAME, TEAMS_TBL, TEAM_ID, team_id)
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
        TEAMS_TBL, TEAM_ID, TEAM_NAME, TEAM_LEADER, TEAM_LAPS)
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
        TAGS_TBL, TAG_ID, TEAM_ID, TAG_TIME, TAG_LAPS)
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

func (ds *DataStore) InsertTags(tags Tags) error {
    err := ds.conn.WithTx(func() error {
        return ds.insertTags(tags)
    })
    if err != nil {
        log.Printf("InsertTags: %v", err)
    }
    return err
}


func (ds *DataStore) GetTagsForTeam(team_id int) (Tags, error) {
    s := fmt.Sprintf("SELECT * FROM %s WHERE %s = %d", TAGS_TBL, TEAM_ID, team_id)
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

        var tag_id, team_id, laps int
        var tag_time string
        err = stmt.Scan(&tag_id, &team_id, &tag_time, &laps)
        if err != nil {
            log.Printf("GetTagsForTeam Scan %v", err)
            return tags, err
        }
        tags = append(tags, &Tag{TeamID: team_id, TagID: tag_id, LastUpdated: tag_time, Laps: laps})
    }
    return tags, err
}

/*
func (ds *DataStore) Populate() {
    var teams Teams
    teams = append(teams, &Team{TeamID: 0, Name: "Foobar", Leader: "Joe Blow"})
    teams = append(teams, &Team{TeamID: 1, Name: "LYB", Leader: "Jacki B"})
    ds.InsertTeams(teams)

    var tags Tags
    tags = append(tags, &Tag{TagID: 123, TeamID: 0})
    tags = append(tags, &Tag{TagID: 124, TeamID: 0})
    tags = append(tags, &Tag{TagID: 125, TeamID: 1})
    tags = append(tags, &Tag{TagID: 126, TeamID: 1})
    ds.InsertTags(tags)

    ds.IncrementLaps(123)
    time.Sleep((MIN_LAP_SECS+1) * time.Second)
    ds.IncrementLaps(123)

    time.Sleep((MIN_LAP_SECS+1) * time.Second)
    ds.IncrementLaps(124)
    time.Sleep((MIN_LAP_SECS+1) * time.Second)
    ds.IncrementLaps(124)
    time.Sleep((MIN_LAP_SECS+1) * time.Second)
    ds.IncrementLaps(124)

    time.Sleep((MIN_LAP_SECS+1) * time.Second)
    ds.IncrementLaps(125)
    time.Sleep((MIN_LAP_SECS+1) * time.Second)
    ds.IncrementLaps(125)
    time.Sleep((MIN_LAP_SECS+1) * time.Second)
    ds.IncrementLaps(125)
    time.Sleep((MIN_LAP_SECS+1) * time.Second)
    ds.IncrementLaps(125)

    if teams, err := ds.GetLeaderboard(2); err == nil {
        log.Printf("teams[0]: %v", teams[0])
    }

    if tags, err := ds.GetTagsForTeam(0); err == nil {
        for _, t := range tags {
            log.Printf("tags for team 0: %v", t)
        }
    }
}


func main() {
    ds := ConnectToDB()
    defer ds.Close()
    ds.Populate()
}
*/