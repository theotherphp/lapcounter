package main

import (
	"flag"
	"log"
)

func main() {
	log.Println("relay starting")

	importFilePtr := flag.String("import", "", "CSV file to import")
	flag.Parse()
	if *importFilePtr != "" {
		ds := ConnectToDB()
		if err := ds.Import(*importFilePtr); err != nil {
			log.Printf("Import %v", err)
		}
		ds.Close()
	}

	StartWebServer()
	log.Println("relay exiting")
}
