# pskreporter
Go based PSKReporter service

##downloading

1. clone the repository onto your system, preferably in your $GOPATH/src.
2. cd $GOPATH/src/github.com/sasimpson/pskreporter
3. go get <- will get all dependencies
3. go build server.go

##usage:
```
  Usage of ./server:
  -batchsize=1000: size of batches to send to rethinkdb
  -batchtimeout=1ns: timeout for batches in seconds
  -dbworkers=10: number of db process workers to use, default: 10
  -processors=100: number of process workers to use, default: 100
  -rethink-host="127.0.0.1:28015": rethinkdb host and port, default: 127.0.0.1:28015
  -createdb: will create the db and tables on rethinkdb connection
```
