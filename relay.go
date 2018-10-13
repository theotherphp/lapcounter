package main

import (
    "log"
)

func main() {
    log.Println("starting")
    ds := InitDataStore()
    defer ds.Close()
    InitWebServer(ds)
    log.Println("exiting")
}