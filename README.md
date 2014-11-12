## 简介

* 为了让异步的请求对于在线业务影响最小，开发了一个在本机的异步缓冲，它会转发异步请求，当远端消费阻塞以后通过顺序写文件来转移

## 使用
* git clone https://github.com/wudikua/mini_async
* sh build.sh
* php test.php

```
<?php
$data = array(
    'host'=>'prism001.m6',
	'port'=>'1807',
	'op'=>'RPUSH',
	'key'=>'new-log',
	'val'=>'999999999'
);
$redis = new Redis();
$redis->connect('127.0.0.1', 9008);
$redis->rPush("buffering-redis", json_encode($data));

```
## 状态监控

* 启动以后通过 localhost:8080/status 获取状态信息

```
{
    conections: 0,  //当前的并发数
    redis_buffering: {  //redis消费队列的消费情况
        aof_util: {     //文件失败转移的情况
            current_offset: 0,  //当前失败的文件末尾
            sendbackOffset: 0   //失败以后成功消费的文件末尾
        },
        data_len: 0,    //正常数据队列长度
        fail_len: 0,    //失败数据队列长度
        failover: false //当前是否是故障状态，fase代表正常
    },
    total_count: 0  //已经接受的请求总数
}
```

## 实现

### Server
* 解析redis的队列协议 http://redis.readthedocs.org/en/latest/topic/protocol.html

* 立刻返回成功，并根据需要转发的task类型放入相应的channel
* redis-buffering是转发redis请求 

### RedisBufferingChannel
* channel满了标记为故障状态，channel消费到空恢复正常状态
* 正常状态直接写channel，故障状态就通过AOFUtil写文件

### AOFUtil
* 顺序写文件，把发送内容前加上4个字节的下一行的位置信息，每写一条更新写到的文件末尾是多少
* 一个线程通过检测最新的文件末尾来向相应的BufferingChannel回发数据
