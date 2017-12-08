package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type Event struct {
	Type         string    `json:"type"`
	Action       string    `json:"action"`
	RegisteredAt time.Time `json:"registered_at"`
	EventId      int       `json:"event_id"`
}

type EventsCollection struct {
	Events []Event
}

type maxIdResponse struct {
	Aggregations struct {
		Max_id struct {
			Value float32
		}
	}
}

func getEvents() {
	resp, err := http.Get(baseUrl + "/events?from=" + getLastId())
	if err != nil {
		evLogger.Printf("Could not GET events: %v", err)
		return
	}
	body, _ := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	events := unmarshalEvents(body)
	insertEvents(events)
}

func unmarshalEvents(eventsStr []byte) (events EventsCollection) {
	if err := json.Unmarshal(eventsStr, &events); err != nil {
		evLogger.Printf("Could not unmarshall %s: %v", eventsStr, err)
		return EventsCollection{}
	}
	return
}

func getLastId() string {
	query := strings.NewReader(`{"aggs": {"max_id": { "max": { "field": "event_id" }}}, "size": 0}`)
	res, err := http.Post(esHost+"/events/_search", "application/json", query)
	if err != nil {
		evLogger.Printf("elasticsearch server is unreachable: %v", err)
		return
	}
	body, _ := ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	var jsonResponse maxIdResponse
	err = json.Unmarshal(body, &jsonResponse)
	return strconv.Itoa(int(jsonResponse.Aggregations.Max_id.Value))
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
