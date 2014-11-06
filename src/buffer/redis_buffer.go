package buffer

import (
	"encoding/json"
	"fmt"
	"github.com/golang/glog"
	"github.com/xuyu/goredis"
	"time"
)

type RedisBufferingEntry struct {
	Host string `json:"host"`
	Port string `json:"port"`
	Op   string `json:"op"`
	Key  string `json:"key"`
	Val  string `json:"val"`
}

type RedisBufferingChannel struct {
	dataChannel  chan string
	failChannel  chan string
	failHelper   *AOFUtil
	redisClients map[string]*goredis.Redis
	failover     bool
	count        int
}

const (
	REDIS_BUFFERING_LENGTH = 1024
)

var RedisBuffer RedisBufferingChannel

func init() {
	RedisBuffer.dataChannel = make(chan string, REDIS_BUFFERING_LENGTH)
	RedisBuffer.failChannel = make(chan string, REDIS_BUFFERING_LENGTH)
	RedisBuffer.count = 0
	RedisBuffer.failover = false
	RedisBuffer.failHelper, _ = NewAOFUtil("fail_redis.db", RedisBuffer.dataChannel, &RedisBuffer.failover)
	go RedisBuffer.Select()
}

func (this *RedisBufferingChannel) Select() {
	glog.Infoln("Redis Buffering Channel Selecting")
	for {
		select {
		case data := <-this.failChannel:
			glog.Infoln("Fail Channel Recv", data)
			this.failHelper.AOFWrite(data)
			break
		case data := <-this.dataChannel:
			glog.Infoln("Data Channel Recv", data)
			// 反序列化REDIS命令
			var entry RedisBufferingEntry
			if err := json.Unmarshal([]byte(data), &entry); err != nil {
				glog.Warningln("Unmarshal Error", err)
			}
			addr := fmt.Sprintf("%s:%s", entry.Host, entry.Port)
			// 从连接池取连接
			redis, err := this.GetRedis(addr)
			if err != nil {
				glog.Warningln("Redis Connect Error", entry.Host, entry.Port)
			} else {
				select {
				case <-time.After(time.Millisecond * 10):
					glog.Warningln("Redis Execute Quick Failed")
					this.dataChannel <- data
					break
				default:
					// 代理执行REDIS指令
					_, err = redis.ExecuteCommand(entry.Op, entry.Key, entry.Val)
					if err != nil {
						glog.Warningln("Execute Commmand Error", err)
						// 重连REDIS
						this.redisClients[addr] = nil
						// 执行失败重新放回队列重试
						this.dataChannel <- data
					}
				}
			}
			// 故障状态
			if len(this.dataChannel) == REDIS_BUFFERING_LENGTH {
				glog.Warningln("Data Channel Blocked")
				this.failover = true
			}
			// 故障恢复已经消费完队列
			if this.failover && len(this.dataChannel) == 0 {
				// 恢复了EnQueue写入
				this.failover = false
			}
			break
		}
	}
}

func (this *RedisBufferingChannel) GetRedis(addr string) (*goredis.Redis, error) {
	glog.Infoln("Redis Connect to", addr)
	if this.redisClients[addr] != nil {
		return this.redisClients[addr], nil
	}
	client, err := goredis.Dial(&goredis.DialConfig{Address: addr})
	if err != nil {
		return nil, err
	}
	return client, nil
}

func (this *RedisBufferingChannel) EnQueue(val string) {
	glog.Infoln("Redis Buffering Recv:", val)
	this.count = this.count + 1
	glog.Infoln("Redis Buffering Recv Count", this.count)
	if false && !this.failover {
		// 正常的消费队列
		this.dataChannel <- val
	} else {
		this.failChannel <- val
	}

}

func (this *RedisBufferingChannel) Status() map[string]interface{} {
	status := make(map[string]interface{})
	status["data_len"] = len(this.dataChannel)
	status["fail_len"] = len(this.failChannel)
	status["failover"] = this.failover
	status["aof_util"] = this.failHelper.Status()
	return status
}

func (this *RedisBufferingChannel) Destory() {
	this.failHelper.Destory()
}
