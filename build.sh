#!/bin/bash
export GOPATH=$(pwd)
rm -rf pkg
rm -rf server
go get "github.com/golang/glog"
go get "github.com/xuyu/goredis"
go get "github.com/bitly/go-simplejson"
go get "github.com/julienschmidt/httprouter"
go build src/server.go
./server -stderrthreshold=INFO