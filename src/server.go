package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"github.com/golang/glog"
	"io"
	"net"
	"strconv"
	"strings"
)

type TaskBufferingServer struct {
	net  string
	host string
	port int
}

var server TaskBufferingServer

func (this *TaskBufferingServer) ListenAndServe() (err error) {
	addr := fmt.Sprintf("%s:%d", this.host, this.port)
	ln, err := net.Listen(this.net, addr)
	if err != nil {
		glog.Errorf("Listen Error: %s\n", err)
		return
	}
	glog.Infof("Server on %s\n", addr)
	for {
		// 接受客户端链接
		conn, err := ln.Accept()
		if err != nil {
			glog.Warningf("Accept Error: %s\n", err)
			continue
		}

		// 处理客户端链接
		go this.ServeClient(conn)
	}
}

func (this *TaskBufferingServer) ServeClient(conn net.Conn) {
	defer conn.Close()
	glog.Infof("Client: %s\n", conn.RemoteAddr())
	key, val, err := parseRequest(conn)
	if err != nil {
		glog.Warningln("Parse ", key, " Error: ", err)
		conn.Close()
		return
	}
	glog.Infoln("Buffering queue:", key)
	glog.Infoln("Buffering data:", val)
	if _, err = conn.Write([]byte(":1\r\n")); err != nil {
		glog.Warningln("Reply Success Error: ", err)
	}
}

func parseRequest(conn io.Reader) (string, string, error) {
	r := bufio.NewReader(conn)
	line, err := r.ReadString('\n')
	glog.Infoln("Request first line:", line)
	if line == "QUIT\r\n" {
		return "", "", errors.New("Client wanna QUIT")
	}

	line, err = r.ReadString('R')
	_, err = r.ReadString('\n')

	// 解析队列名
	key, err := parseArg(r)
	glog.Infoln("Recv key:", key)
	if err != nil {
		return "", "", err
	}
	// 解析队列数据
	_, err = r.ReadString('\n')
	val, err := parseArg(r)
	if err != nil {
		return key, "", err
	}
	glog.Infoln("Recv val:", val)

	return key, val, nil
}

func parseArg(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	arglen, err := strconv.Atoi(strings.Trim(line, "\\$\r\n"))
	if err != nil {
		return "", err
	}
	glog.Infoln("Arg Length:", arglen)
	buf := make([]byte, arglen)
	nread, err := r.Read(buf)
	if err != nil || nread != arglen {
		glog.Warningln("Read err:", err)
		return "", err
	}
	return string(buf), nil
}

func init() {
	flag.StringVar(&server.net, "net", "tcp", "net")
	flag.StringVar(&server.host, "h", "127.0.0.1", "host")
	flag.IntVar(&server.port, "p", 9008, "port")
	flag.Parse()
}

func main() {
	defer glog.Flush()
	server.ListenAndServe()
}
