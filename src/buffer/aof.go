package buffer

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/golang/glog"
	"os"
	"time"
)

type AOFUtil struct {
	filepath       string
	fis            *bufio.Writer
	fos            *bufio.Reader
	curOffset      uint32
	sendbackOffset uint32
	sourceFailed   *bool
	callbackChan   chan string
}

func NewAOFUtil(filepath string, callback chan string, sourceFailed *bool) (*AOFUtil, error) {
	fin, err := os.OpenFile(filepath, os.O_CREATE|os.O_RDWR, 0660)
	glog.Infoln("Create Failover DB File", filepath)
	if err != nil {
		glog.Fatalln("Create Failover DB File failed")
		return &AOFUtil{}, err
	}
	fout, err := os.OpenFile(filepath, os.O_RDWR, 0660)
	fis := bufio.NewWriter(fin)
	fos := bufio.NewReader(fout)
	aof := AOFUtil{
		filepath:       filepath,
		fis:            fis,
		fos:            fos,
		sourceFailed:   sourceFailed,
		callbackChan:   callback,
		curOffset:      0,
		sendbackOffset: 0,
	}
	go aof.AOFTestingAndSendback()
	return &aof, nil
}

func (this *AOFUtil) AOFWrite(val string) {
	nextOffset := uint32(this.curOffset) + uint32(len([]byte(val))) + uint32(4)
	glog.Infoln("Set nextOffset", nextOffset)
	writeInt32(this.fis, nextOffset)
	glog.Infoln("Write header", nextOffset)
	this.fis.WriteString(val)
	this.fis.Flush()
	this.curOffset = nextOffset
}

func writeInt32(writer *bufio.Writer, i uint32) {
	bs := make([]byte, 4)
	binary.BigEndian.PutUint32(bs, i)
	glog.Infoln("Convert ", i, "To", bs)
	for _, b := range bs {
		writer.WriteByte(b)
	}
}

func readInt32(reader *bufio.Reader) uint32 {
	bs := make([]byte, 4)
	for i, _ := range bs {
		c, err := reader.ReadByte()
		if err != nil {
			glog.Warningln("No Byte Read avaliable", err)
		}
		bs[i] = c
	}
	glog.Infoln("Read Byte", bs)
	return binary.BigEndian.Uint32(bs)
}

func (this *AOFUtil) Status() map[string]interface{} {
	status := make(map[string]interface{})
	status["current_offset"] = this.curOffset
	status["sendbackOffset"] = this.sendbackOffset
	return status
}

func (this *AOFUtil) Destory() {
	this.fis.Flush()
	os.Rename(this.filepath, fmt.Sprintf("%s_old", this.filepath))
}

func (this *AOFUtil) AOFTestingAndSendback() {
	for {
		select {
		case <-time.After(time.Second * 3):
			datalen := len(this.callbackChan)
			glog.Infoln("Testing send offset", this.sendbackOffset, "cur offset", this.curOffset, "data channel len", datalen)
			for this.sendbackOffset < this.curOffset {
				nextOffset := readInt32(this.fos)
				glog.Infoln("Read NextOffset", nextOffset)
				datalen := nextOffset - this.sendbackOffset - 4
				glog.Infoln("Read Row length", datalen)
				val := make([]byte, datalen)
				nread, _ := this.fos.Read(val)
				if nread < int(datalen) {
					this.sendbackOffset = this.curOffset
					break
				}
				this.sendbackOffset = nextOffset
				glog.Infoln("SendbackOffset currOffset", this.sendbackOffset, this.curOffset)
				glog.Infoln("Read Row Val", string(val))
				this.callbackChan <- string(val)
			}
			break
		}
	}
}
