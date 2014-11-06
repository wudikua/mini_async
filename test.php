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

