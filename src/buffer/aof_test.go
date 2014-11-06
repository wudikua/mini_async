package buffer

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/golang/glog"
	"os"
	"testing"
)

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

func TestAOFTestingAndSendback(t *testing.T) {
	fin, _ := os.Open("fail_redis.db")
	r := bufio.NewReader(fin)
	begin := uint32(0)
	end := uint32(5185)
	for begin < end {
		i := readInt32(r)
		fmt.Println(i)
		datalen := i - begin - 4
		val := make([]byte, datalen)
		r.Read(val)
		fmt.Println(string(val))
		begin = i
	}
}
