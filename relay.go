package main

import (
	"flag"
	"log"
	"os"
)

func main() {
	// Open the log file
	logFile, err := os.OpenFile("relay.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Opening relay.log: %v", err)
	}
	defer logFile.Close()
	log.SetOutput(logFile)
	log.Println("")
	log.Println("relay starting")

	// Parse command-line flags
	importFilePtr := flag.String("import", "", "CSV file to import")
	hour := flag.Int("hour", 0, "What hour of the event is it? 1 for 9-10am, etc.")
	flag.Parse()
	if *importFilePtr != "" {
		ds, err := ConnectToDB()
		if err != nil {
			log.Println("main ConnectToDB: ", err)
			return
		}
		if err := ds.Import(*importFilePtr); err != nil {
			log.Println("Import: ", err)
			return
		}
		ds.Close()
	}

	// Ready to go
	StartWebServer(*hour)
	log.Println("relay exiting")
}
