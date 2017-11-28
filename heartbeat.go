// Pings a server every 5 seconds and gets events from it
package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"
)

type Event struct {
	Type, Action, Timestamp string
	Id                      int
}

type EventsCollection struct {
	Events []Event
}

var baseUrl string

func main() {
	baseUrl = os.Getenv("BASE_URL")
	go electricityPing()

	f, logger := setupLogger("events")
	defer f.Close()
	t := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-t.C:
			getLightEvents(logger)
		}
	}
}

func getLightEvents(l *log.Logger) {
	resp, err := http.Get(baseUrl + "/lights")
	if err != nil {
		log.Fatalf("Could not GET events: %v", err)
	}
	eventsStr, _ := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	var events EventsCollection
	if err := json.Unmarshal(eventsStr, &events); err != nil {
		l.Printf("Could not unmarshall %s: %v", eventsStr, err)
	}
	fmt.Println(events)
}

func electricityPing() {
	f, logger := setupLogger("electricityPing")
	defer f.Close()
	url := baseUrl + "/electricity"
	t := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-t.C:
			resp, err := http.Get(url)
			if err != nil {
				logger.Printf("error getting %v: %v", url, err)
			}
			logger.Printf("Call to %v responded with %v", url, resp.StatusCode)
		}
	}
}

func setupLogger(prefix string) (*os.File, *log.Logger) {
	f, err := os.OpenFile(prefix+".log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("could not initiate logger for %v: %v", prefix, err)
	}
	logFlags := log.Ldate | log.Ltime | log.Llongfile | log.LUTC
	return f, log.New(f, prefix, logFlags)
}
