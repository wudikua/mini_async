package main

import (
	"buffer"
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/golang/glog"
	"github.com/julienschmidt/httprouter"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
)

type TaskBufferingServer struct {
	net   string
	host  string
	port  int
	count int
	// 同时可以处理的请求数量，大于这个数将拒绝
	goCount int
}

var server TaskBufferingServer

func (this *TaskBufferingServer) ListenAndServe() (err error) {
	this.count = 0
	this.goCount = 0
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
			conn.Close()
			continue
		}

		if err := this.CanAccept(); err != nil {
			glog.Warningf("Goroutine use up\n")
			conn.Close()
			continue
		}

		// 处理客户端链接
		go this.ServeClient(conn)
	}
}

func (this *TaskBufferingServer) ServeClient(conn net.Conn) {
	glog.Infof("Client: %s\n", conn.RemoteAddr())
	defer conn.Close()
	defer this.FinishRequest()
	r := bufio.NewReader(conn)
	// 每个长连接对应一个线程
	for {
		key, val, err := parseRequest(r)
		if err != nil {
			glog.Warningln("Parse", key, "Error:", err)
			return
		}
		glog.Infoln("Buffering queue:", key)
		glog.Infoln("Buffering data:", val)

		this.DispactherTask(key, val)

		if _, err = conn.Write([]byte(":1\r\n")); err != nil {
			glog.Warningln("Reply Success Error: ", err)
			return
		}
	}
}

func (this *TaskBufferingServer) DispactherTask(key string, val string) {
	this.count = this.count + 1
	glog.Infoln("Accept Count", this.count)
	switch key {
	case "redis-buffering":
		buffer.RedisBuffer.EnQueue(val)
		break
	case "fcgi-buffering":
	case "http-buffering":
	case "moa-buffering":
	}
}

func (this *TaskBufferingServer) Destory() {
	buffer.RedisBuffer.Destory()
}

func (this *TaskBufferingServer) CanAccept() error {
	if this.goCount > 1024 {
		return errors.New("Conn Use up")
	}
	this.goCount = this.goCount + 1
	return nil
}

func (this *TaskBufferingServer) FinishRequest() error {
	this.goCount = this.goCount - 1
	return nil
}

func parseRequest(r *bufio.Reader) (string, string, error) {
	line, err := r.ReadString('\n')

	if err != nil || line == "QUIT\r\n" {
		return "", "", errors.New("Client wanna QUIT")
	}

	glog.Infoln("Request first line:", line)

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

func sigHandler() {
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGUSR1, syscall.SIGUSR2, syscall.SIGHUP, os.Interrupt)
	glog.Infoln("Sig Handler Register")
	for {
		select {
		case sig := <-ch:
			glog.Infoln("Recv sig", sig)
			server.Destory()
			os.Exit(0)
			break
		}
	}
}

func Status(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	status := make(map[string]interface{})
	status["conections"] = server.goCount
	status["total_count"] = server.count
	status["redis_buffering"] = buffer.RedisBuffer.Status()
	b, _ := json.Marshal(status)

	w.Write(b)
}

func main() {
	defer glog.Flush()
	// 处理信号量
	go sigHandler()

	// 服务状态信息
	router := httprouter.New()
	router.GET("/status", Status)
	http.ListenAndServe(":8080", router)

	server.ListenAndServe()
}
