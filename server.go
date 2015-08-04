package main

import (
	"flag"
	"log"
	"net"
	"time"

	r "github.com/dancannon/gorethink"
	"github.com/sasimpson/ipfix"
)

var (
	rethinkServer       string
	rethinkWorkers      int
	processWorkers      int
	batchSize           = 1000
	batchTimeoutSeconds = 1
	session             *r.Session
)

// Report data structure
type report struct {
	Sender               string    `gorethink:"senderCallsign"`
	Receiver             string    `gorethink:"receiverCallsign,omitempty"`
	SenderLocator        string    `gorethink:"senderLocator"`
	ReceiverLocator      string    `gorethink:"receiverLocator,omitempty"`
	Frequency            int32     `gorethink:"frequency"`
	Mode                 string    `gorethink:"mode"`
	SNR                  uint8     `gorethink:"snr,omitempty"`
	IMD                  uint8     `gorethink:"imd,omitempty"`
	DecoderSoftware      string    `gorethink:"decoderSoftware,omitempty"`
	AntennaInformation   string    `gorethink:"antennaInformation,omitempty"`
	InformationSource    int8      `gorethink:"informationSource,omitempty"`
	PersistentIdentifier string    `gorethink:"persistentIdentifier,omitempty"`
	FlowStartSeconds     time.Time `gorethink:"flowStartSeconds,omitempty"`
}

// CheckError check error function
func CheckError(fail bool, err error) {
	if err != nil {
		if fail {
			log.Fatalln(err)
		} else {
			log.Println(err)
		}
	}
}

func serviceListener() {
	//set up worker pools and processes
	processWorkerPool := make(chan []byte, 1000)
	rethinkWorkerPool := make(chan report, 1000)
	for rt := 0; rt <= rethinkWorkers; rt++ {
		go rethinkWorker(rt, rethinkWorkerPool)
	}
	for i := 0; i <= processWorkers; i++ {
		go processData(i, processWorkerPool, rethinkWorkerPool)
	}
	//setup UDP listenter
	buf := make([]byte, 2048)
	addr, err := net.ResolveUDPAddr("udp", "0.0.0.0:8081")
	sock, err := net.ListenUDP("udp", addr)
	CheckError(true, err)
	counter := 0
	//listen
	for {
		rlen, _, err := sock.ReadFromUDP(buf)
		CheckError(true, err)
		readData := make([]byte, rlen)
		copy(readData, buf[0:rlen])
		processWorkerPool <- readData
		counter++
	}
}

// processData takes the data from the job channel, processes it and puts it on
// the rethink worker pool (rtWP)
func processData(w int, jobs <-chan []byte, rtWP chan<- report) {
	for bufferData := range jobs {
		s := ipfix.NewSession()
		i := ipfix.NewInterpreter(s)

		i.AddDictionaryEntry(ipfix.DictionaryEntry{Name: "senderCallsign", FieldID: 1, EnterpriseID: 30351, Type: ipfix.String})
		i.AddDictionaryEntry(ipfix.DictionaryEntry{Name: "receiverCallsign", FieldID: 2, EnterpriseID: 30351, Type: ipfix.String})
		i.AddDictionaryEntry(ipfix.DictionaryEntry{Name: "senderLocator", FieldID: 3, EnterpriseID: 30351, Type: ipfix.String})
		i.AddDictionaryEntry(ipfix.DictionaryEntry{Name: "receiverLocator", FieldID: 4, EnterpriseID: 30351, Type: ipfix.String})
		i.AddDictionaryEntry(ipfix.DictionaryEntry{Name: "frequency", FieldID: 5, EnterpriseID: 30351, Type: ipfix.Int32})
		i.AddDictionaryEntry(ipfix.DictionaryEntry{Name: "sNR", FieldID: 6, EnterpriseID: 30351, Type: ipfix.Uint8})
		i.AddDictionaryEntry(ipfix.DictionaryEntry{Name: "iMD", FieldID: 7, EnterpriseID: 30351, Type: ipfix.Uint8})
		i.AddDictionaryEntry(ipfix.DictionaryEntry{Name: "decoderSoftware", FieldID: 8, EnterpriseID: 30351, Type: ipfix.String})
		i.AddDictionaryEntry(ipfix.DictionaryEntry{Name: "antennaInformation", FieldID: 9, EnterpriseID: 30351, Type: ipfix.String})
		i.AddDictionaryEntry(ipfix.DictionaryEntry{Name: "mode", FieldID: 10, EnterpriseID: 30351, Type: ipfix.String})
		i.AddDictionaryEntry(ipfix.DictionaryEntry{Name: "informationSource", FieldID: 11, EnterpriseID: 30351, Type: ipfix.Int8})
		i.AddDictionaryEntry(ipfix.DictionaryEntry{Name: "persistentIdentifier", FieldID: 12, EnterpriseID: 30351, Type: ipfix.String})
		i.AddDictionaryEntry(ipfix.DictionaryEntry{Name: "flowStartSeconds", FieldID: 150, Type: ipfix.DateTimeSeconds})

		msg, err := s.ParseBuffer(bufferData)
		CheckError(false, err)

		var fieldList []ipfix.InterpretedField
		for _, record := range msg.DataRecords {
			fieldList = i.InterpretInto(record, fieldList)

			var data report

			for _, f := range fieldList {
				if f.EnterpriseID == 30351 {
					switch f.FieldID {
					case 1:
						data.Sender = f.Value.(string)
					case 2:
						data.Receiver = f.Value.(string)
					case 3:
						data.SenderLocator = f.Value.(string)
					case 4:
						data.ReceiverLocator = f.Value.(string)
					case 5:
						data.Frequency = f.Value.(int32)
					case 6:
						data.SNR = f.Value.(uint8)
					case 7:
						data.IMD = f.Value.(uint8)
					case 8:
						data.DecoderSoftware = f.Value.(string)
					case 9:
						data.AntennaInformation = f.Value.(string)
					case 10:
						data.Mode = f.Value.(string)
					case 11:
						data.InformationSource = f.Value.(int8)
					case 12:
						data.PersistentIdentifier = f.Value.(string)
					}
				} else {
					switch f.FieldID {
					case 150:
						data.FlowStartSeconds = f.Value.(time.Time)
					}
				}
			}
			rtWP <- data
		}
	}
}

// rethinkWorker batches the data into quantities specified in batchSize then saves
// to the database when the batchSize or batchTimeoutSeconds is hit.
func rethinkWorker(w int, jobs <-chan report) {
	listOfStuff := make([]report, 0, batchSize)
	for {
		select {
		case data := <-jobs:
			listOfStuff = append(listOfStuff, data)
			if len(listOfStuff) >= batchSize {
				_, err := r.Table("pskreport").Insert(listOfStuff).RunWrite(session)
				CheckError(true, err)
				listOfStuff = make([]report, 0, batchSize)
				continue
			}
		case <-time.After(time.Second * time.Duration(batchTimeoutSeconds)):
			if len(listOfStuff) != 0 {
				_, err := r.Table("pskreport").Insert(listOfStuff).RunWrite(session)
				CheckError(true, err)
				listOfStuff = make([]report, 0, batchSize)
				continue
			}
		}
	}
}

func main() {
	var err error

	flag.IntVar(&processWorkers, "processors", 100, "number of process workers to use, default: 100")
	flag.IntVar(&rethinkWorkers, "dbworkers", 10, "number of db process workers to use, default: 10")
	flag.StringVar(&rethinkServer, "rethink-host", "127.0.0.1:28015", "rethinkdb host and port, default: 127.0.0.1:28015")
	flag.Parse()

	session, err = r.Connect(r.ConnectOpts{
		Address:  rethinkServer,
		Database: "radio",
		MaxIdle:  10,
		MaxOpen:  200,
	})

	CheckError(true, err)
	serviceListener()
}
