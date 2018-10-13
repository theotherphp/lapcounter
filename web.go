package main 

import (
    "context"
    "html/template"
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

type Notification struct {
    tagKey int
}

type WebServer struct {
    ds *DataStore
    tagChannel chan int
    notifyChannel chan struct{}
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
    if r.Method == "GET" {
        if teamKey, err := strconv.Atoi(r.URL.Path[len("/team/"):]); err == nil {
            type TeamParam struct {
                Name string
                Tags []*Tag
            }

            svr.runTemplate(w, "./templates/team.html",
                TeamParam {
                    Name: svr.ds.GetTeam(teamKey).Name,
                    Tags: svr.ds.GetTagsByTeam(teamKey),
            })
        }
    } else if r.Method == "POST" {
        log.Println("/team/ POST unimplemented")
    }
}


func (svr *WebServer) handleTeams(w http.ResponseWriter, r *http.Request) {
    if r.Method == "GET" {
        type TeamsParam struct {
            Teams []*Team
        }
        svr.runTemplate(w, "./templates/teams.html",
            TeamsParam{Teams: svr.ds.GetTeams()})
    } else if r.Method == "POST" {
        log.Println("/teams/ POST unimplemented")
    }
}


var upgrader = websocket.Upgrader{}


func (svr *WebServer) handleLaps(w http.ResponseWriter, r *http.Request) {
    conn, err := upgrader.Upgrade(w, r, nil)
    if err != nil {  // Separate line so conn is in scope for goroutine below
        log.Println("/laps/ upgrader.Upgrade: ", err)
        return
    }
    defer conn.Close()

    // Publish tag reads to the tag channel
    go func() {
        log.Println("producing to tagChannel")
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
            }
        }
    }()

    // Consume the tag channel, updating the data store
    go func() {
        log.Println("consuming tagChannel")
        for {
            tagKey := <-svr.tagChannel
            svr.ds.IncrementLaps(tagKey)
        }
    }()
}


func (svr *WebServer) handleNotify(w http.ResponseWriter, r *http.Request) {
    conn, err := upgrader.Upgrade(w, r, nil)
    if err != nil {  // Separate line so conn is in scope for goroutine below
        log.Println("/notify/ upgrader.Upgrade ", err)
    }

    // Consume the notification channel, sending to the browser client
    go func() {
        log.Println("consuming notifyChannel")
        for n := range svr.notifyChannel {
            conn.WriteJSON(n)
        }
    }()
}


func InitWebServer(ds *DataStore) {
    svr := new(WebServer)
    svr.ds = ds
    svr.tagChannel = make(chan int)
    svr.notifyChannel = make(chan struct{})

    var httpsvr http.Server
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
    http.HandleFunc("/notify/", svr.handleNotify)

    if err := httpsvr.ListenAndServe(); err != http.ErrServerClosed {
        log.Println("http.ListenAndServe: ", err)
    }
}
