package main

import (
	"log"
	"strings"
	"time"
)

// HourTicker waits until a specified time to start an hourly ticker
func HourTicker(hour int, startHrsMins string, update chan uint, quit chan bool) {
	// Calculate the Time at which we should start the hourly ticker
	now := time.Now()
	elems := strings.Split(now.Format(time.RFC822), " ") // e.g. "18 Nov 18 09:05 PST"
	if len(startHrsMins) > 0 {
		elems[3] = startHrsMins
	}
	then, err := time.Parse(time.RFC822, strings.Join(elems, " "))
	if err != nil {
		log.Println("time.Parse: ", err)
		return
	}
	tilStart := then.Sub(now)

	time.AfterFunc(tilStart, func() {
		tick := time.NewTicker(time.Minute * 1).C
		hourBit := uint(1) << uint(hour)
		update <- hourBit
		for {
			select {
			case <-tick:
				hourBit = hourBit << 1
				update <- hourBit
			case <-quit:
				log.Println("HourTicker exiting")
				return
			}
		}
	})
}
