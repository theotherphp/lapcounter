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

	StartWebServer()
	log.Println("relay exiting")
}
