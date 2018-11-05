package main

import (
	"context"
	"time"

	"html/template"
	// "encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"

	"github.com/gorilla/websocket"
)

/*
   Theory of Operation
   1. There are RESTful routes to support the admin web pages for teams and tags
   2. There are Gorilla Websockets to support the RFID tag readers and the TV display
   3. Gorountines and channels support concurrent non-blocking Websockets
*/

type NotifyClient struct {
	conn    *websocket.Conn
	maxRank int
	// chanForThisConn chan Notification
}

type WebServer struct {
	ds                *DataStore
	tagChannel        chan int
	quitTagChannel    chan bool
	notifyChannel     chan struct{}
	quitNotifyChannel chan bool
	notifyClients     map[*websocket.Conn]*NotifyClient
}

func (svr *WebServer) handleRoot(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/teams/", http.StatusSeeOther)
}

func (svr *WebServer) runTemplate(w http.ResponseWriter, name string, param interface{}) {
	if tmpl, err := template.ParseFiles(name); err == nil {
		if err = tmpl.Execute(w, param); err != nil {
			log.Println("template.Execute ", name, err)
		}
	} else {
		log.Println("template.Parsefiles ", name, err)
	}
}

func (svr *WebServer) handleTeam(w http.ResponseWriter, r *http.Request) {
	ds := ConnectToDB()
	defer ds.Close()

	if r.Method == "GET" {
		if teamKey, err := strconv.Atoi(r.URL.Path[len("/team/"):]); err == nil {
			type TeamParam struct {
				Name string
				Tags []*Tag
			}

			var tags Tags
			if tags, err = ds.GetTagsForTeam(teamKey); err != nil {
				log.Println("GetTagsForTeam: ", err)
			}
			var name string
			if name, err = ds.GetTeamName(teamKey); err != nil {
				log.Println("GetTagsForTeam: ", err)
			}
			svr.runTemplate(w, "./templates/team.html",
				TeamParam{
					Name: name,
					Tags: tags,
				})
		}
	} else if r.Method == "POST" {
		log.Println("/team/ POST unimplemented")
	}
}

func (svr *WebServer) handleTeams(w http.ResponseWriter, r *http.Request) {
	ds := ConnectToDB()
	defer ds.Close()

	if r.Method == "GET" {
		type TeamsParam struct {
			Teams []*Team
		}

		if teams, err := ds.GetTeams(); err == nil {
			svr.runTemplate(w, "./templates/teams.html",
				TeamsParam{Teams: teams})
		}
	} else if r.Method == "POST" {
		log.Println("/teams/ POST unimplemented")
	}
}

var upgrader = websocket.Upgrader{}

func (svr *WebServer) handleLaps(w http.ResponseWriter, r *http.Request) {
	log.Println("handleLaps starting")
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil { // Separate line so conn is in scope for goroutine below
		log.Println("/laps/ upgrader.Upgrade: ", err)
		return
	}
	defer conn.Close()

	// Publish tag reads to the tag channel
	for {
		if _, msg, err := conn.ReadMessage(); err == nil {
			tagKey, err := strconv.Atoi(string(msg))
			if err == nil {
				svr.tagChannel <- tagKey
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

func (svr *WebServer) serviceTagChannel() {
	// Consume the tag channel, updating the data store
	log.Println("serviceTagChannel starting")
	ds := ConnectToDB()
	defer ds.Close()

	for {
		select {
		case tagKey := <-svr.tagChannel:
			ds.IncrementLaps(tagKey)
		case <-svr.quitTagChannel:
			log.Println("serviceTagChannel exiting")
			return
		}
	}
}

func (svr *WebServer) serviceNotifyChannel() {
	log.Println("serviceNotifyChannel starting")
	for {
		select {
		case notif := <-svr.notifyChannel:
			log.Println("Notif: ", notif)
		case <-svr.quitNotifyChannel:
			log.Println("serviceTagChannel exiting")
			return
		}
	}
}

/*
func (svr *WebServer) handleNotify(w http.ResponseWriter, r *http.Request) {
    conn, err := upgrader.Upgrade(w, r, nil)
    if err != nil {  // Separate line so conn is in scope for goroutine below
        log.Println("/notify/ upgrader.Upgrade ", err)
    }

    maxRank, err := strconv.Atoi(r.URL.Query().Get("maxRank"))
    if err != nil {
        maxRank = -1
    }
    ch := make(chan Notification, 10)
    cli := NotifyClient{
        maxRank: maxRank,
        chanForThisConn: ch,
        conn: conn}
    svr.notifyClients[conn] = &cli
    go svr.notify(&cli)
}


func (svr *WebServer) notify(cli *NotifyClient) {
    for {
        notification := <- cli.chanForThisConn
        if cli.maxRank > 0 && notification.team_rank > cli.maxRank {
            // This client is a leaderboard, interested in the top N teams
            continue
        }
        if payload, err := json.marshal(notification); err != nil {
            fmt.Println("json.marshal: ", err)
            continue
        }
        if err := cli.conn.WriteMessage(websocket.TextMessage, payload); err != nil {
            fmt.Println("WriteMessage: ", err)
            delete(svr.notifyClients, cli.conn)
            break
        }
    }
}
*/

// StartWebServer starts and stops the app and its goroutines
func StartWebServer() {
	svr := new(WebServer)
	svr.tagChannel = make(chan int, 10)
	svr.quitTagChannel = make(chan bool)
	svr.notifyChannel = make(chan struct{}, 10)
	svr.quitNotifyChannel = make(chan bool)

	/*
	   svr.notifyChannel = make(chan Notification, 100)
	   svr.notifyClients = make(map[*Conn]NotifyClient)
	*/
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
	http.HandleFunc("/teams/", svr.handleTeams)
	http.HandleFunc("/laps/", svr.handleLaps)
	// http.HandleFunc("/notify/", svr.handleNotify)
	http.Handle("/templates/", http.StripPrefix("/templates/", http.FileServer(http.Dir("./templates"))))

	go svr.serviceTagChannel()
	go svr.serviceNotifyChannel()

	if err := httpsvr.ListenAndServe(); err != http.ErrServerClosed {
		log.Println("http.ListenAndServe: ", err)
	}
	svr.quitTagChannel <- true
	svr.quitNotifyChannel <- true

	// Wait for quit goroutines to close the DB cleanly
	// I thought unbuffered channels were synchronous so this seems odd
	time.Sleep(time.Second)
}
