package main

import (
	"encoding/hex"
	"flag"
	"log"
	"net"
	"time"

	r "github.com/dancannon/gorethink"
	// "github.com/sasimpson/ipfix"
	"github.com/calmh/ipfix"
)

var (
	rethinkServer  string
	rethinkWorkers int
	processWorkers int
	batchSize      int
	batchTimeout   time.Duration
	session        *r.Session
)

type packet struct {
	Data   []byte
	Sender net.Addr
	Size   int
}

// Report data structure
type report struct {
	Sender               string `gorethink:"senderCallsign"`
	Receiver             string `gorethink:"receiverCallsign"`
	SenderLocator        string `gorethink:"senderLocator"`
	ReceiverLocator      string `gorethink:"receiverLocator"`
	Frequency            int32  `gorethink:"frequency"`
	Mode                 string `gorethink:"mode"`
	SNR                  uint8  `gorethink:"snr"`
	IMD                  uint8  `gorethink:"imd"`
	DecoderSoftware      string `gorethink:"decoderSoftware"`
	AntennaInformation   string `gorethink:"antennaInformation"`
	InformationSource    int8   `gorethink:"informationSource"`
	PersistentIdentifier string `gorethink:"persistentIdentifier"`
	FlowStartSeconds     time.Time
	SelectionSequenceID  int8
	FlowID               uint64
	SenderAddr           string
	RawData              string
}

// error report data structure
type dataError struct {
	BinaryData   string    `gorethink:"binaryData"`
	LengthOfData int       `gorethink:"lengthOfData"`
	CreatedAt    time.Time `gorethink:"createdAt"`
	ErrorType    string    `gorethink:"errorType"`
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
	processWorkerPool := make(chan *packet, processWorkers)
	rethinkWorkerPool := make(chan *report, rethinkWorkers)
	for rt := 0; rt <= rethinkWorkers; rt++ {
		go rethinkWorker(rt, rethinkWorkerPool)
	}
	for i := 0; i <= processWorkers; i++ {
		go processData(i, processWorkerPool, rethinkWorkerPool)
	}
	//setup UDP listenter

	addr, err := net.ResolveUDPAddr("udp", "0.0.0.0:8081")
	sock, err := net.ListenUDP("udp", addr)
	CheckError(true, err)
	//listen
	for {
		buf := make([]byte, 4096)
		rawDataPacket := new(packet)
		rawDataPacket.Size, rawDataPacket.Sender, err = sock.ReadFromUDP(buf)
		rawDataPacket.Data = make([]byte, rawDataPacket.Size)
		CheckError(true, err)
		copy(rawDataPacket.Data, buf[0:rawDataPacket.Size])
		processWorkerPool <- rawDataPacket
	}
}

// processData takes the data from the job channel, processes it and puts it on
// the rethink worker pool (rtWP)
func processData(w int, jobs <-chan *packet, rtWP chan<- *report) {
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
		i.AddDictionaryEntry(ipfix.DictionaryEntry{Name: "flowID", FieldID: 148, Type: ipfix.Uint64})
		i.AddDictionaryEntry(ipfix.DictionaryEntry{Name: "selectionSequenceID", FieldID: 301, Type: ipfix.Uint8})

		msg, err := s.ParseBuffer(bufferData.Data)
		if err != nil {
			r.Table("errors").Insert(dataError{
				BinaryData:   hex.EncodeToString(bufferData.Data),
				LengthOfData: bufferData.Size,
				CreatedAt:    time.Now(),
				ErrorType:    "ipfix",
			}).RunWrite(session)
			// log.Println(err	)
		} else {
			// log.Println(bufferData)
		}
		// CheckError(false, err)

		var fieldList []ipfix.InterpretedField
		for _, record := range msg.DataRecords {
			fieldList = i.InterpretInto(record, fieldList)

			var data report
			data.SenderAddr = bufferData.Sender.String()
			data.RawData = hex.EncodeToString(bufferData.Data)

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
					case 148:
						data.FlowID = f.Value.(uint64)
					case 150:
						data.FlowStartSeconds = f.Value.(time.Time)
					case 301:
						data.SelectionSequenceID = f.Value.(int8)
					}

				}
			}
			//throw reference to the report struct on the rethink queue.
			rtWP <- &data
		}
	}
}

// rethinkWorker batches the data into quantities specified in batchSize then saves
// to the database when the batchSize or batchTimeoutSeconds is hit.
func rethinkWorker(w int, jobs <-chan *report) {
	listOfStuff := make([]report, 0, batchSize)
	for {
		select {
		case data := <-jobs:
			listOfStuff = append(listOfStuff, *data)
			if len(listOfStuff) >= batchSize {
				_, err := r.Table("report").Insert(listOfStuff).RunWrite(session)
				CheckError(true, err)
				listOfStuff = make([]report, 0, batchSize)
				continue
			}
		case <-time.After(time.Second * time.Duration(batchTimeout)):
			if len(listOfStuff) != 0 {
				_, err := r.Table("report").Insert(listOfStuff).RunWrite(session)
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
	flag.IntVar(&batchSize, "batchsize", 1000, "size of batches to send to rethinkdb")
	flag.DurationVar(&batchTimeout, "batchtimeout", 1, "timeout for batches in seconds")
	flag.Parse()
	log.Println("processes: ", processWorkers, "; db workers: ", rethinkWorkers, "; batch size: ", batchSize)

	session, err = r.Connect(r.ConnectOpts{
		Address:  rethinkServer,
		Database: "pskreporter",
		MaxIdle:  10,
		MaxOpen:  200,
	})
	CheckError(true, err)
	serviceListener()
}
