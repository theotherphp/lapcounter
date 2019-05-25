package main

import (
	"flag"
	"log"
	"os"
	"time"
)

func main() {
	// Open the log file
	logFile, err := os.OpenFile("relay.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Opening relay.log: %v", err)
	}
	defer logFile.Close()
	log.SetOutput(logFile)
	log.Println("starting up")

	// Parse command-line flags
	importFilePtr := flag.String("import", "", "CSV file to import")
	hour := flag.Uint("hour", 0, "What hour of the event is it? 0 for 9-10am, etc.") // Use in case of restart mid-event
	start := flag.String("start", "0s", "Wait until start, e.g. \"2h25m\"")          // Use to set up gear before official start time
	flag.Parse()
	if *importFilePtr != "" {
		ds, err := ConnectToDB()
		if err != nil {
			log.Fatalln("main ConnectToDB: ", err)
		}
		if err := ds.Import(*importFilePtr); err != nil {
			log.Fatalln("Import: ", err)
		}
		ds.Close()
	}
	tilStart, err := time.ParseDuration(*start)
	if err != nil {
		log.Fatalln("Bad duration: ", *start)
	}

	// Ready to rumble
	StartWebServer(*hour, tilStart)
	log.Println("clean shutdown")
}
