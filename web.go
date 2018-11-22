package main

import (
	"context"
	"encoding/json"
	"html/template"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

/*
   Theory of Operation
   1. There are RESTful routes to support the admin web pages for teams and tags
   2. There are Gorilla Websockets to support the RFID tag readers and the TV display
   3. No blocking and no locking. Everything is goroutines and channels
   4. There are one-per-connection goroutines to "handle" incoming lap/tag counts and outgoing notifications
   5. There are singleton goroutines to "service" the channels which mediate cross-goroutine communication
*/

type notifyClient struct {
	send chan Notification // if I knew how to make a channel of channels I wouldn't need this
}

type webServer struct {
	ds *DataStore

	// Incoming tag reads
	tags     chan int
	quitTags chan bool

	// Outgoing notifications
	notify     chan Notification
	quitNotify chan bool
	register   chan *notifyClient
	unregister chan *notifyClient

	// Hourly time updates to support clients/hours.html
	updateHour     chan uint
	quitUpdateHour chan bool
}

func (svr *webServer) handleRoot(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/teams/", http.StatusSeeOther)
}

func (svr *webServer) runTemplate(w http.ResponseWriter, name string, param interface{}) {
	if tmpl, err := template.ParseFiles(name); err == nil {
		if err = tmpl.Execute(w, param); err != nil {
			log.Println("template.Execute ", name, err)
		}
	} else {
		log.Println("template.Parsefiles ", name, err)
	}
}

func reportError(w http.ResponseWriter, err error, logHint string) {
	log.Println(logHint, err)
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

func (svr *webServer) handleTeam(w http.ResponseWriter, r *http.Request) {
	ds, err := ConnectToDB()
	if err != nil {
		reportError(w, err, "ConnectToDB /team: ")
		return
	}
	defer ds.Close()

	if r.Method == "GET" {
		if teamKey, err := strconv.Atoi(r.URL.Path[len("/team/"):]); err == nil {
			type TeamParam struct {
				Name   string
				Tags   []*Tag
				TeamID int
			}

			tags, err := ds.GetTagsForTeam(teamKey)
			if err != nil {
				reportError(w, err, "GetTagsForTeam: ")
				return
			}
			team, err := ds.GetOneTeam(teamKey)
			if err != nil {
				reportError(w, err, "GetOneTeam: ")
				return
			}
			svr.runTemplate(w, "./templates/team.html",
				TeamParam{
					Name:   team.Name,
					Tags:   tags,
					TeamID: team.TeamID,
				})
		}
	} else if r.Method == "POST" { // Add tags to this team
		r.ParseForm()
		var err error
		var teamID, first, last int
		if teamID, err = strconv.Atoi(r.FormValue("team_id")); err != nil {
			reportError(w, err, "atoi team_id")
			return
		}
		firstLast := strings.Split(r.FormValue("tag_ids"), "-")
		if first, err = strconv.Atoi(firstLast[0]); err != nil {
			reportError(w, err, "atoi firstLast[0]")
			return
		}
		if len(firstLast) > 1 {
			if last, err = strconv.Atoi(firstLast[1]); err != nil {
				reportError(w, err, "Atoi firstLast[1]")
				return
			}
		} else {
			last = first
		}
		var tags Tags
		for tagID := first; tagID <= last; tagID++ {
			tags = append(tags, &Tag{TagID: tagID, TeamID: teamID})
		}
		if err = ds.insertTags(tags); err != nil {
			reportError(w, err, "insertTags: ")
			return
		}
		http.Redirect(w, r, "/team/"+strconv.Itoa(teamID), http.StatusFound)
	}
}

func (svr *webServer) handleTeams(w http.ResponseWriter, r *http.Request) {
	ds, err := ConnectToDB()
	if err != nil {
		reportError(w, err, "ConnectToDB /teams/: ")
		return
	}
	defer ds.Close()

	if r.Method == "GET" {
		type TeamsParam struct {
			Teams []*Team
			Laps  int
			Miles float64
		}

		teams, err := ds.GetTeams()
		if err != nil {
			reportError(w, err, "GetTeams: ")
			return
		}
		const lapsToMiles = 400 * 3.28084 / 5280
		laps := 0
		for _, t := range teams {
			laps += t.Laps
		}
		svr.runTemplate(w, "./templates/teams.html",
			TeamsParam{
				Teams: teams,
				Laps:  laps,
				Miles: float64(laps) * lapsToMiles,
			})
	} else if r.Method == "POST" {
		log.Println("/teams/ POST unimplemented")
	}
}

// handleHours gives an AJAX interface (rather than document/template) to GetTeams
// It's used by the hours.html client
func (svr *webServer) handleHours(w http.ResponseWriter, r *http.Request) {
	ds, err := ConnectToDB()
	if err != nil {
		reportError(w, err, "/hours ConnectToDB: ")
		return
	}
	defer ds.Close()

	if r.Method == "GET" {
		teams, err := ds.GetTeams()
		if err != nil {
			reportError(w, err, "/hours GetTeams: ")
			return
		}
		teamsJSON, err := json.Marshal(teams)
		if err != nil {
			reportError(w, err, "json.Marshal: ")
			return
		}
		w.Write(teamsJSON)
	} else if r.Method == "POST" {
		reportError(w, err, "/hours POST unimplemented")
	}
}

var upgrader = websocket.Upgrader{}

// handleLaps is the HTTP websocket handler for incoming tag reads from the RFID readers
func (svr *webServer) handleLaps(w http.ResponseWriter, r *http.Request) {
	log.Println("handleLaps starting")
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("/laps/ upgrader.Upgrade: ", err)
		return
	}
	defer conn.Close()

	for {
		if _, msg, err := conn.ReadMessage(); err == nil {
			tagID, err := strconv.Atoi(string(msg))
			if err == nil {
				svr.tags <- tagID // Publish tag reads to the tag channel
			} else {
				log.Println("strconv.Atoi: ", msg)
			}
		} else {
			log.Println("conn.ReadMessage: ", err)
			break
		}
	}
	log.Println("handleLaps exiting")
}

// serviceTagChannel consumes the tag channel, allowing DB updates to be async with incoming tag reads
func (svr *webServer) serviceTagChannel() {
	log.Println("serviceTagChannel starting")
	ds, err := ConnectToDB()
	if err != nil {
		log.Println("tagChannel ConnectToDB: ", err)
		return
	}
	defer ds.Close()

	var hourBit uint
	for {
		select {
		case tagKey := <-svr.tags: // Consume the tag channel
			if notif, err := ds.IncrementLaps(tagKey, hourBit); err == nil {
				svr.notify <- notif // Publish notification to the clients
			} else {
				log.Println("IncrementLaps: ", err)
			}
		case <-svr.quitTags:
			log.Println("serviceTagChannel exiting")
			return
		case hourBit = <-svr.updateHour:
			log.Println("hourBit: ", hourBit)
		}
	}
}

// handleNotify is the HTTP websocket handler for browser clients to receive notifications
func (svr *webServer) handleNotify(w http.ResponseWriter, r *http.Request) {
	log.Println("handleNotify starting")
	upgrader.CheckOrigin = func(r *http.Request) bool {
		return true
	}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("/notify upgrader.Upgrade ", err)
		return
	}

	client := &notifyClient{send: make(chan Notification, 10)}
	svr.register <- client
	for {
		select {
		case notif := <-client.send:
			// send the notification to the browser client
			if err := conn.WriteJSON(notif); err != nil {
				log.Println("handleNotify write failed, returning")
				svr.unregister <- client
				return
			}
		}
	}
}

// serviceNotifyChannel is a waystation for notifications between the DB and the /notify handlers
// it also provides a concurrency-safe map to fan out notifications to many clients
func (svr *webServer) serviceNotifyChannel() {
	log.Println("serviceNotifyChannel starting")
	clients := make(map[*notifyClient]bool)

	for {
		select {
		case r := <-svr.register:
			clients[r] = true
		case ur := <-svr.unregister:
			delete(clients, ur)
		case notif := <-svr.notify:
			for client := range clients {
				client.send <- notif // send the notification to running /notify handlers
			}
		case <-svr.quitNotify:
			log.Println("serviceNotifyChannel exiting")
			return
		}
	}
}

// StartWebServer starts and stops the app and its goroutines
func StartWebServer(hour int) {
	svr := &webServer{
		tags:           make(chan int, 10),
		quitTags:       make(chan bool),
		notify:         make(chan Notification, 10),
		quitNotify:     make(chan bool),
		register:       make(chan *notifyClient),
		unregister:     make(chan *notifyClient),
		updateHour:     make(chan uint),
		quitUpdateHour: make(chan bool),
	}

	var httpsvr http.Server
	httpsvr.Addr = ":8080"
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	go func() {
		<-quit
		log.Println("received os.Interrupt")
		if err := httpsvr.Shutdown(context.Background()); err != nil {
			log.Fatalf("Shutdown: %v\n", err)
		}
	}()

	http.HandleFunc("/", svr.handleRoot)
	http.HandleFunc("/team/", svr.handleTeam)
	http.HandleFunc("/teams", svr.handleTeams)
	http.HandleFunc("/hours", svr.handleHours)
	http.HandleFunc("/laps", svr.handleLaps)
	http.HandleFunc("/notify", svr.handleNotify)
	http.Handle("/templates/", http.StripPrefix("/templates/", http.FileServer(http.Dir("./templates"))))
	http.Handle("/clients/", http.StripPrefix("/clients/", http.FileServer(http.Dir("./clients"))))

	go svr.serviceTagChannel()
	go svr.serviceNotifyChannel()
	go HourTicker(hour, "", svr.updateHour, svr.quitUpdateHour)
	if err := httpsvr.ListenAndServe(); err != http.ErrServerClosed {
		log.Println("http.ListenAndServe: ", err)
	}
	svr.quitTags <- true
	svr.quitNotify <- true
	svr.quitUpdateHour <- true

	// Wait for goroutines to quit so we close the DB cleanly
	// I thought unbuffered channels were synchronous so this seems odd
	time.Sleep(time.Second)
}
