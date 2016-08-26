package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"

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

var taskID uint64
var commandChan = make(chan string, 100)

var tasksState = make(map[string]*TaskStatus)

var stateFile = fmt.Sprintf("%s/%s", os.TempDir(), "state.json")
var frameworkInfoFile = fmt.Sprintf("%s/%s", os.TempDir(), "framewrok.json")

func main() {
	user := "root"
	name := "simple_framework"
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}
	listen := ":9090"
	webuiURL := fmt.Sprintf("http://%s%s", hostname, listen)
	failoverTimeout := float64(3600)
	checkpoint := true
	frameworkJSON, err := ioutil.ReadFile(frameworkInfoFile)
	if err == nil {
		jsonpb.UnmarshalString(string(frameworkJSON), &frameworkInfo)
	} else {
		frameworkInfo = FrameworkInfo{
			User:            &user,
			Name:            &name,
			Hostname:        &hostname,
			WebuiUrl:        &webuiURL,
			FailoverTimeout: &failoverTimeout,
			Checkpoint:      &checkpoint,
		}
	}

	http.HandleFunc("/", web)
	go func() {
		log.Fatal(http.ListenAndServe(listen, nil))
	}()

	log.Fatal(subscribe())
}

func web(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	switch r.Method {
	case "POST":
		cmd := r.Form["cmd"][0]
		commandChan <- cmd
		w.WriteHeader(http.StatusAccepted)
		fmt.Fprintf(w, "Scheduled: %s", cmd)
	case "DELETE":
		id := r.Form["id"][0]
		err := kill(id)
		if err != nil {
			fmt.Fprint(w, err)
		} else {
			fmt.Print(w, "KILLED")
		}
	case "GET":
		stateJSON, _ := json.Marshal(tasksState)
		w.Header().Add("Content-type", "application/json")
		fmt.Fprint(w, string(stateJSON))
	default:
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
	}
}

func kill(id string) error {
	update, ok := tasksState[id]
	log.Printf("Kill task %s [%#v]", id, update)
	if !ok {
		return fmt.Errorf("Unknown task %s", id)
	}
	return call(&Call{
		Type: Call_KILL.Enum(),
		Kill: &Call_Kill{
			TaskId:  update.TaskId,
			AgentId: update.AgentId,
		},
	})
}

func subscribe() error {
	subscribeCall := &Call{
		FrameworkId: frameworkInfo.Id,
		Type:        Call_SUBSCRIBE.Enum(),
		Subscribe:   &Call_Subscribe{FrameworkInfo: &frameworkInfo},
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
		if bytesCount == 0 {
			continue
		}
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
			json, _ := marshaller.MarshalToString(&frameworkInfo)
			ioutil.WriteFile(frameworkInfoFile, []byte(json), 0644)
			reconcile()
		case Event_HEARTBEAT:
			log.Print("PING")
		case Event_OFFERS:
			log.Printf("Handle offers returns: %v", handleOffers(event.Offers))
		case Event_UPDATE:
			log.Printf("Handle update returns: %v", handleUpdate(event.Update))
		}
	}
}

func reconcile() {
	oldState, err := ioutil.ReadFile(stateFile)
	if err == nil {
		json.Unmarshal(oldState, &tasksState)
	}
	var oldTasks []*Call_Reconcile_Task
	maxID := 0
	for _, t := range tasksState {
		oldTasks = append(oldTasks, &Call_Reconcile_Task{
			TaskId:  t.TaskId,
			AgentId: t.AgentId,
		})
		numericID, err := strconv.Atoi(t.TaskId.GetValue())
		if err == nil && numericID > maxID {
			maxID = numericID
		}
	}
	atomic.StoreUint64(&taskID, uint64(maxID))
	call(&Call{
		Type:      Call_RECONCILE.Enum(),
		Reconcile: &Call_Reconcile{Tasks: oldTasks},
	})
}

func handleUpdate(update *Event_Update) error {
	tasksState[update.Status.TaskId.GetValue()] = update.Status
	stateJSON, _ := json.Marshal(tasksState)
	ioutil.WriteFile(stateFile, stateJSON, 0644)
	return call(&Call{
		Type: Call_ACKNOWLEDGE.Enum(),
		Acknowledge: &Call_Acknowledge{
			AgentId: update.Status.AgentId,
			TaskId:  update.Status.TaskId,
			Uuid:    update.Status.Uuid,
		},
	})
}

func handleOffers(offers *Event_Offers) error {

	offerIds := []*OfferID{}
	for _, offer := range offers.Offers {
		offerIds = append(offerIds, offer.Id)
	}

	select {
	case cmd := <-commandChan:
		firstOffer := offers.Offers[0]

		TRUE := true
		newTaskID := fmt.Sprint(atomic.AddUint64(&taskID, 1))
		taskInfo := []*TaskInfo{{
			Name: &cmd,
			TaskId: &TaskID{
				Value: &newTaskID,
			},
			AgentId:   firstOffer.AgentId,
			Resources: defaultResources(),
			Command: &CommandInfo{
				Shell: &TRUE,
				Value: &cmd,
			}}}
		accept := &Call{
			Type: Call_ACCEPT.Enum(),
			Accept: &Call_Accept{
				OfferIds: offerIds,
				Operations: []*Offer_Operation{{
					Type: Offer_Operation_LAUNCH.Enum(),
					Launch: &Offer_Operation_Launch{
						TaskInfos: taskInfo,
					}}}}}
		return call(accept)
	default:
		decline := &Call{
			Type:    Call_DECLINE.Enum(),
			Decline: &Call_Decline{OfferIds: offerIds},
		}
		return call(decline)
	}
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

func defaultResources() []*Resource {
	CPU := "cpus"
	MEM := "mem"
	cpu := float64(0.1)

	return []*Resource{
		{
			Name:   &CPU,
			Type:   Value_SCALAR.Enum(),
			Scalar: &Value_Scalar{Value: &cpu},
		},
		{
			Name:   &MEM,
			Type:   Value_SCALAR.Enum(),
			Scalar: &Value_Scalar{Value: &cpu},
		},
	}
}
