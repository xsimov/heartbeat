// Tickers to get events from a server and ping it for electricity
package main

import (
	"log"
	"net/http"
	"os"
	"time"
)

var (
	baseUrl, esHost, arduinoHost string
	evLogger                     *log.Logger
)

func main() {
	baseUrl = os.Getenv("BASE_URL")
	esHost = os.Getenv("ES_HOST")
	arduinoHost = os.Getenv("ARDUINO_HOST")

	go electricityPing()

	var f *os.File
	f, evLogger = setupLogger("events")
	defer f.Close()

	t := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-t.C:
			ec := getEvents()
			insertEvents(ec)
			processEvents(ec)
		}
	}
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
				continue
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
