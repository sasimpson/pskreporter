package main

import (
	"log"
	"net"
	"time"

	r "github.com/dancannon/gorethink"
	"github.com/sasimpson/ipfix"
)

var (
	session *r.Session
)

// Report data structure
type Report struct {
	sender    string  `gorethink:"senderCallsign,omitempty"`
	receiver  string  `gorethink:"receiverCallsign,omitempty"`
	frequency float32 `gorethink:"frequency,omitempty"`
	mode      string  `gorethink:"mode,omitempty"`
}

type myStruct struct {
	data map[string]interface{}
}

// CheckError check error function
func CheckError(fail bool, err error) {
	if err != nil {
		if fail {
			log.Fatalln(err)
		} else {
			// log.Println(err)
		}
	}
}

func serviceListener() {
	jobs := make(chan []byte, 1000)
	rethinkWorkerPool := make(chan myStruct, 1000)
	workers := 100
	rtworkers := 10
	for rt := 0; rt <= rtworkers; rt++ {
		go rethinkWorker(rt, rethinkWorkerPool)
	}

	addr, err := net.ResolveUDPAddr("udp", "0.0.0.0:8081")
	sock, err := net.ListenUDP("udp", addr)
	CheckError(true, err)
	buf := make([]byte, 2048)
	for i := 0; i <= workers; i++ {
		go processData(i, jobs, rethinkWorkerPool)
	}
	counter := 0
	for {
		rlen, _, err := sock.ReadFromUDP(buf)
		CheckError(true, err)
		readData := make([]byte, rlen)
		copy(readData, buf[0:rlen])
		jobs <- readData
		counter++
	}
}

func processData(w int, jobs <-chan []byte, rtWP chan<- myStruct) {
	for bufferData := range jobs {
		s := ipfix.NewSession()
		i := ipfix.NewInterpreter(s)

		i.AddDictionaryEntry(ipfix.DictionaryEntry{Name: "senderCallsign", FieldID: 1, EnterpriseID: 30351, Type: ipfix.String})
		i.AddDictionaryEntry(ipfix.DictionaryEntry{Name: "receiverCallsign", FieldID: 2, EnterpriseID: 30351, Type: ipfix.String})
		i.AddDictionaryEntry(ipfix.DictionaryEntry{Name: "senderLocation", FieldID: 3, EnterpriseID: 30351, Type: ipfix.String})
		i.AddDictionaryEntry(ipfix.DictionaryEntry{Name: "receiverLocation", FieldID: 4, EnterpriseID: 30351, Type: ipfix.String})
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

			var data = map[string]interface{}{}

			for _, f := range fieldList {
				if f.EnterpriseID == 30351 {
					switch f.FieldID {
					case 1:
						data["sender"] = f.Value.(string)
					case 2:
						data["receiver"] = f.Value.(string)
					case 3:
						data["sender_location"] = f.Value.(string)
					case 4:
						data["receiver_location"] = f.Value.(string)
					case 5:
						data["frequency"] = f.Value.(int32)
					case 6:
						data["snr"] = f.Value.(uint8)
					case 7:
						data["imd"] = f.Value.(uint8)
					case 8:
						data["decoder_software"] = f.Value.(string)
					case 9:
						data["antenna_information"] = f.Value.(string)
					case 10:
						data["mode"] = f.Value.(string)
					case 11:
						data["information_source"] = f.Value.(int8)
					case 12:
						data["persistent_identifier"] = f.Value.(string)
					}
				} else {
					switch f.FieldID {
					case 150:
						data["flow_start_seconds"] = f.Value.(time.Time)
						// default:
						// log.Printf("non-conforming enterprise id: [%v] => [%v][%v]", f.Name, f.Value, f.EnterpriseID)
					}

				}
			}
			// log.Println(data)
			rtWP <- myStruct{data: data}
		}
	}
}

func rethinkWorker(w int, jobs <-chan myStruct) {
	listOfStuff := []map[string]interface{}{}
	for {
		select {
		case data1 := <-jobs:
			listOfStuff = append(listOfStuff, data1.data)
			if len(listOfStuff) >= 10000 {
				wr, err := r.Table("pskreport").Insert(listOfStuff).RunWrite(session)
				log.Println(w, wr)
				CheckError(true, err)
				listOfStuff = []map[string]interface{}{}
				continue
			}
		case <-time.After(time.Second * 1):
			if len(listOfStuff) != 0 {
				wr, err := r.Table("pskreport").Insert(listOfStuff).RunWrite(session)
				log.Println(w, wr)
				CheckError(true, err)
				listOfStuff = []map[string]interface{}{}
				continue
			}
		}
	}
}

func main() {
	var err error

	session, err = r.Connect(r.ConnectOpts{
		Address:  "127.0.0.1:28015",
		Database: "radio",
		MaxIdle:  10,
		MaxOpen:  201,
	})

	CheckError(true, err)
	serviceListener()
}
