package main

import (
	"bufio"
	"bytes"
	"log"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/golang/protobuf/jsonpb"
)

// Url to Mesos master scheduler API
const schedulerApiUrl = "http://10.10.10.10:5050/api/v1/scheduler"

// Current framework configuration
var frameworkInfo FrameworkInfo
var mesosStreamID string

// Marshaler to serialize Protobuf Message to JSON
var marshaller = jsonpb.Marshaler{
	EnumsAsInts: false,
	Indent:      "  ",
	OrigName:    true,
}

func main() {
	user := "root"
	name := "simple_framework"
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}
	frameworkInfo = FrameworkInfo{
		User:            &user,
		Name:            &name,
		Hostname:        &hostname,
	}

	log.Fatal(subscribe())
}

func subscribe() error {
	subscribeCall := &Call{
		Type:      Call_SUBSCRIBE.Enum(),
		Subscribe: &Call_Subscribe{FrameworkInfo: &frameworkInfo},
	}
	body, _ := marshaller.MarshalToString(subscribeCall)
	log.Print(body)
	res, _ := http.Post(schedulerApiUrl, "application/json", bytes.NewBuffer([]byte(body)))
	defer res.Body.Close()

	reader := bufio.NewReader(res.Body)
	// Read line from Mesos
	line, _ := reader.ReadString('\n')
	// First line contains numbers of message bytes
	bytesCount, _ := strconv.Atoi(strings.Trim(line, "\n"))
	// Event loop
	for {
		// Read line from Mesos
		line, _ = reader.ReadString('\n')
		line = strings.Trim(line, "\n")
		// Read important data
		data := line[:bytesCount]
		// Rest data will be bytes of next message
		bytesCount, _ = strconv.Atoi((line[bytesCount:]))
		// Do not handle events, just log them
		log.Printf("Got: [%s]", data)

		var event Event
		jsonpb.UnmarshalString(data, &event)
		log.Printf("Got: [%s]", event.String())
	
		switch *event.Type {
		case Event_SUBSCRIBED:
			log.Print("Subscribed")
			frameworkInfo.Id = event.Subscribed.FrameworkId
			mesosStreamID = res.Header.Get("Mesos-Stream-Id")
		case Event_HEARTBEAT:
			log.Print("PING")
		case Event_OFFERS:
			log.Printf("Handle offers returns: %v", handleOffers(event.Offers))
		}
	}
}

func handleOffers(offers *Event_Offers) error {
		offerIds := []*OfferID{}
		for _, offer := range offers.Offers {
			offerIds = append(offerIds, offer.Id)
		}
		decline := &Call{
			Type:    Call_DECLINE.Enum(),
			Decline: &Call_Decline{OfferIds: offerIds},
		}
		return call(decline)
}

func call(message *Call) error {
	message.FrameworkId = frameworkInfo.Id
	body, _ := marshaller.MarshalToString(message)
	req, _ := http.NewRequest("POST", schedulerApiUrl, bytes.NewBuffer([]byte(body)))	
	req.Header.Set("Mesos-Stream-Id", mesosStreamID)
	req.Header.Set("Content-Type", "application/json")
	log.Printf("Call %s %s", message.Type, string(body))
	res, _ := http.DefaultClient.Do(req)
	defer res.Body.Close()
	if res.StatusCode != 202 {
		io.Copy(os.Stderr, res.Body)
		return fmt.Errorf("Error %d", res.StatusCode)
	}
	return nil
}
