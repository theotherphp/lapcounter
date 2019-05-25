package main

import (
	"log"
	"time"
)

// HourTicker waits until a specified time to start an hourly ticker
func HourTicker(hour uint, tilStart time.Duration, update chan uint, quit chan bool) {
	log.Println("waiting until ", time.Now().Add(tilStart).Format("3:04PM"))
	time.AfterFunc(tilStart, func() {
		tick := time.NewTicker(time.Hour).C
		update <- hour
		for {
			select {
			case <-tick:
				hour++
				update <- hour
			case <-quit:
				log.Println("stop counting hours")
				return
			}
		}
	})
}
