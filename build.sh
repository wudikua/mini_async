#!/bin/bash
export GOPATH=$(pwd)
rm -rf pkg
rm -rf server
go get "github.com/golang/glog"
go build src/server.go
./server -stderrthreshold=INFO