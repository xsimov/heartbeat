package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"
)

type Event struct {
	Type, Action string
	Timestamp    time.Time
}

type EventsCollection struct {
	Events []Event
}

var evLogger *log.Logger

func getEvents(l *log.Logger) {
	evLogger = l
	resp, err := http.Get(baseUrl + "/events")
	if err != nil {
		evLogger.Printf("Could not GET events: %v", err)
		return
	}
	eventsStr, _ := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	var events EventsCollection
	if err := json.Unmarshal(eventsStr, &events); err != nil {
		evLogger.Printf("Could not unmarshall %s: %v", eventsStr, err)
		return
	}
	insertEvents(events)
}

func insertEvents(ec EventsCollection) {
	var bulk string
	for _, e := range ec.Events {
		jsonStr, err := json.Marshal(e)
		if err != nil {
			evLogger.Printf("could not marshal %v: %v", e, err)
			return
		}
		bulk += fmt.Sprintf(`{"index": {"_type": %q}}`, e.Type)
		bulk += "\n" + string(jsonStr) + "\n"
	}
	bulkInsert(bulk)
}

func bulkInsert(b string) {
	res, err := http.Post(esHost+"/events/_bulk", "application/json", strings.NewReader(b))
	if err != nil {
		evLogger.Printf("elasticsearch server is unreachable: %v", err)
		return
	}
	bd, _ := ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	evLogger.Println(res.StatusCode, string(bd))
}
