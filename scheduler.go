package main

import (
	"bufio"
	"bytes"
	"log"
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
	}
}
