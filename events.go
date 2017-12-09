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

func getEvents() EventsCollection {
	resp, err := http.Get(baseUrl + "/events?from=" + getLastId())
	if err != nil {
		evLogger.Printf("could not GET events: %v", err)
		return EventsCollection{}
	}
	body, _ := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	return unmarshalEvents(body)
}

func unmarshalEvents(eventsStr []byte) (events EventsCollection) {
	if err := json.Unmarshal(eventsStr, &events); err != nil {
		evLogger.Printf("could not unmarshall %s: %v", eventsStr, err)
		return EventsCollection{}
	}
	return
}

func processEvents(ec EventsCollection) {
	for _, e := range ec.Events {
		url := composeEventURL(e)
		_, err := http.Get(url)
		if err != nil {
			evLogger.Printf("could not process event %v: %v", e, err)
			return
		}
	}
}

func composeEventURL(e Event) string {
	action := 0
	if e.Action == "on" {
		action = 1
	}
	return arduinoHost + "/" + e.Type + "/4/" + strconv.Itoa(action)
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

func getLastId() string {
	query := strings.NewReader(`{"aggs": {"max_id": { "max": { "field": "event_id" }}}, "size": 0}`)
	res, err := http.Post(esHost+"/events/_search", "application/json", query)
	if err != nil {
		evLogger.Printf("elasticsearch server is unreachable: %v", err)
		return ""
	}
	body, _ := ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	var jR maxIdResponse
	err := json.Unmarshal(body, &jR)
	if err != nil {
		evLogger.Printf("could not unmarshal ES response: %q", body)
		return ""
	}
	return strconv.Itoa(int(jR.Aggregations.Max_id.Value))
}
