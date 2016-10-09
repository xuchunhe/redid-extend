<?php
/* *
 * Redis 扩展类 （主要供评论功能使用) 
 * @author xuchunhe
 */

class Redis_RedisExt {

	private $config, $handle;
	
	public function __construct() {
		$this->config = Sr::config('cache.drivers.redis.config');	
		if (empty($this->config['slaves']) && !empty($this->config['masters'])) {
			$this->config['slaves'][] = current($this->config['masters']);
		}
	}
	
	/**
	 * 主redis初始化
	 */
	private function _initMaters() {
		if (empty($this->handle['masters'])) {
			$this->handle['masters'] = array();
			//array('masters', 'slaves')
			foreach ($this->config['masters'] as $k => $config) {
				$this->handle['masters'][$k] = new Redis();
				if ($config['type'] == 'sock') {
					$this->handle['masters'][$k]->connect($config['sock']);
				} else {
					$this->handle['masters'][$k]->connect($config['host'], $config['port'], $config['timeout'], $config['retry']);
				}
				if (!is_null($config['password'])) {
					$this->handle['masters'][$k]->auth($config['password']);
				}
				if (!is_null($config['prefix'])) {
					if ($config['prefix']{strlen($config['prefix']) - 1} != ':') {
						$config['prefix'].=':';
					}
					$this->handle['masters'][$k]->setOption(Redis::OPT_PREFIX, $config['prefix']);
				}
				$this->handle['masters'][$k]->select($config['db']);
			}
		}
	}
	
	/**
	 * 从redis初始化
	 */
	private function _initSlave() {
		if (empty($this->handle['slave'])) {
			//随机选取一个从redis配置，然后连接
			$config = $this->config['slaves'][array_rand($this->config['slaves'])];
			$this->handle['slave'] = new Redis();
			if ($config['type'] == 'sock') {
				$this->handle['slave']->connect($config['sock']);
			} else {
				$this->handle['slave']->connect($config['host'], $config['port'], $config['timeout'], $config['retry']);
			}
			if (!is_null($config['password'])) {
				$this->handle['slave']->auth($config['password']);
			}
			if (!is_null($config['prefix'])) {
				if ($config['prefix']{strlen($config['prefix']) - 1} != ':') {
					$config['prefix'].=':';
				}
				$this->handle['slave']->setOption(Redis::OPT_PREFIX, $config['prefix']);
			}
			$this->handle['slave']->select($config['db']);
		}
	}
	
	/**
	 * 清空全部keys
	 * @return int [1|0] 1:成功 0：失败
	 */
	public function clean() {
		$this->_initMaters();
		$status = true;
		foreach ($this->handle['masters'] as $k => $handle) {
			$status = $status & $this->handle['masters'][$k]->flushDB();
		}
		return $status;
	}
	
	/**
	 * json_encode
	 */
	private function _encode($data) {
		return json_encode($data);
	}
	
	/**
	 * json_decode
	 */
	private function _decode($data) {
		return json_decode($data, true);
	}
	
	/********************************* Redis Command *************************/
	
	/*********** about strings ************/
	
	/**
	 * 根据key获取值
	 * @param  string $key 
	 * @return mix
	 */
	public function get($key) {
		$this->_initSlave();
		if ($data = $this->handle['slave']->get($key)) {
			return $this->_decode($data);
		} else {
			return null;
		}
	}
	
	/**
	 * 设置key的值
	 * @param string $key 
	 * @param mix $value 存放的数据
	 * @param int $cacheTime 过期时间（秒）
	 * @return int [1:成功|0:失败] 
	 */
	public function set($key, $value, $cacheTime = 0) {
		$this->_initMaters();
		$value = $this->_encode($value);
		$status = true;
		foreach ($this->handle['masters'] as $k => $v) {
			if ($cacheTime) {
				$status = $status & $this->handle['masters'][$k]->setex($key, $cacheTime, $value);
			} else {
				$status = $status & $this->handle['masters'][$k]->set($key, $value);
			}
		}
		return $status;
	}
	
	/**
	 * 对key对应的数字做加1操作。
	 *
	 * @tips 如果key不存在，那么在操作之前，这个key对应的值会被置为0。
	 * 如果key有一个错误类型的value或者是一个不能表示成数字的字符串，
	 * 就返回错误。这个操作最大支持在64位有符号的整型数字。
	 * 
	 * @param  string  $key
	 * @param  int  [$offset] 偏移量
	 * @return int 增加之后的value
	 */
	public function incr($key, $offset = 1) {
		$offset = (int)$offset;
		$this->_initMaters();
		$value = 0;
		foreach ($this->handle['masters'] as $k => $v) {
			if ($offset != 1) {
				$value = $this->handle['masters'][$k]->incrBy($key, $offset);
			} else {
				$value = $this->handle['masters'][$k]->incr($key);
			}
		}
		return $value;
	}
	
	/**
	 * 对key对应的数字做减1操作。
	 *
	 * @tips 如果key不存在，那么在操作之前，这个key对应的值会被置为0。
	 * 如果key有一个错误类型的value或者是一个不能表示成数字的字符串，
	 * 就返回错误。这个操作最大支持在64位有符号的整型数字。
	 * 
	 * @param  string  $key
	 * @param  int  [$offset] 偏移量
	 * @return int 减小之后的value
	 */
	public function decr($key, $offset = 1) {
		$offset = (int)$offset;
		$this->_initMaters();
		$value = 0;
		foreach ($this->handle['masters'] as $k => $v) {
			if ($offset != 1) {
				$value = $this->handle['masters'][$k]->decrBy($key, $offset);
			} else {
				$value = $this->handle['masters'][$k]->decr($key);
			}
		}
		return $value;
	}
	
	/************************* about keys **********************/
	
	/**
	 * 删除一个key或多个key
	 * 支持del(key1 key2 key3 ......)
	 * @param  string $key
	 * @return int 被删除key的数量
	 */
	public function del($key) {
		$this->_initMaters();
		$args = func_get_args();
		$nums = 0;
		foreach ($this->handle['masters'] as $k => $v) {
			$nums = call_user_func_array(array($this->handle['masters'][$k], 'del'), $args);
		}
		return $nums;
	}
	
	/**
	 * 检察某一个key是否存在
	 * @param  string $key
	 * @return boolean [true|false]
	 */
	public function exists($key) {
		$this->_initSlave();
		$result = $this->handle['slave']->exists($key);
		return $result;
	}
	
	/**
	 * 返回key所存储的值的类型
	 * @param  string $key
	 * @return int [none|string|list|set|zset|hash]
	 * 
	 * string: Redis::REDIS_STRING  1
	 * set: Redis::REDIS_SET  2
	 * list: Redis::REDIS_LIST  3
	 * zset: Redis::REDIS_ZSET  4
	 * hash: Redis::REDIS_HASH  5
	 * other: Redis::REDIS_NOT_FOUND  0
	 */
	public function type($key) {
		$this->_initSlave();
		$result = $this->handle['slave']->type($key);
		return $result;
	}
	
	/**
	 * 获取keys
	 * @param mix $pattern 匹配的内容
	 * @return array
	 */
	public function keys($pattern = '*') {
		$this->_initSlave();
		$result = $this->handle['slave']->keys($pattern);
		return $result;
	}
	
	/****** about sorted sets *******/
	
	/**
	 * 添加到有序set的一个或多个成员，或更新的分数，如果它已经存在
	 *
	 * 支持zadd($key, $score, $member, [$score, $member] ...)
	 *
	 * member传入之前需要json_encode
	 * 
	 * @param  string  $key
	 * @param  float $score 分数/权重
	 * @param  string $member 成员值
	 * @return int 添加成员的数量
	 */
	public function zAdd($key, $score, $member) {
		$this->_initMaters();
		$args = func_get_args();	
		foreach ($args as $k => $v){
			$args[$k] = $k <> 0 && $k % 2 == 0 ? $this->_encode($v) : $v;
		}
		$nums = 0;
		foreach ($this->handle['masters'] as $k => $v) {
			$nums = call_user_func_array(array($this->handle['masters'][$k], 'zadd'), $args);
		}
		return $nums;
	}
	
	/**
	 * 返回有序集中，指定区间内的成员【根据score升序，具有相同分数值的成员按字典序(lexicographical order )来排列】
	 * @param  string  $key
	 * @param  integer $start：区间开始位置
	 * @param  integer $end：区间结束位置
	 * 【start 和 end 都以 0 表示有序集第一个成员，以 1 表示第二个成员，以此类推；也可以用负数，-1 表示最后一个成员，-2 表示倒数第二个成员，以此类推】
	 * @param  boolean $withscores member的值将作为键值
	 * @return array 有序数组
	 */
	public function zRange($key, $start = 0, $end = -1, $withscores = false) {
		$this->_initSlave();
		$res = $this->handle['slave']->zrange($key, $start, $end, $withscores);
		$return = array();
		if (true === is_array($res)) {
			foreach ($res as $k => $v) {
				if($withscores){
					$return[$this->_decode($k)] = $v;
				}else{
					$return[$k] = $this->_decode($v);
				}
			}
		}
		return $return;
	}
	
	/**
	 * 返回有序集中，指定区间内的成员【根据score降序，具有相同分数值的成员按字典序(lexicographical order )来排列】
	 * @param  string  $key
	 * @param  integer $start
	 * @param  integer $end
	 * @param  boolean $withscores member的值将作为键值
	 * @return array 有序数组
	 */
	public function zRevRange($key, $start = 0, $end = -1, $withscores = false) {
		$this->_initSlave();
		$res = $this->handle['slave']->zrevrange($key, $start, $end, $withscores);
		$return = array();
		if (true === is_array($res)) {
			foreach ($res as $k => $v) {
				if($withscores){
					$return[$this->_decode($k)] = $v;
				}else{
					$return[$k] = $this->_decode($v);
				}
			}
		}
		return $return;
	}
	
	/**
	 * 从排序的集合中删除一个或多个成员
	 * 支持zrem($key, key member [member ...])
	 * @param  string  $key
	 * @param  mix $member 成员值
	 * @return int 被删除的成员数量
	 */
	public function zRem($key, $member) {
		$this->_initMaters();
		$args = func_get_args();
		foreach($args as $k => $v){
			$args[$k] = $k > 0 ? $this->_encode($v) : $v;
		}
		$nums = 0;
		foreach ($this->handle['masters'] as $k => $v) {
			$nums = call_user_func_array(array($this->handle['masters'][$k], 'zrem'), $args);
		}
		return $nums;
	}
	
	/**
	 * 获取一个排序的集合中的成员数量
	 * @param  string  $key
	 * @return int 成员总数 (key不存在返回0)
	 */
	public function zCard($key) {
		$this->_initSlave();
		$res = $this->handle['slave']->zcard($key);
		return $res;
	}
	
	/**
	 * 返回在排序集合成员的score
	 * @tips（可以用来判断member是否存在is_float）
	 * @param  string  $key
	 * @param  mix $member 成员值
	 * @return mix [float|false] 存在返回float类型，不存在返回false
	 */
	public function zScore($key, $member) {
		$member = $this->_encode($member);
		$this->_initSlave();
		$res = $this->handle['slave']->zscore($key, $member);
		return $res;
	}
	
	/**
	 * 根据score从低到高，返回member在有续集中的index
	 * @tips（可以用来判断member是否存在is_int）
	 * @param  string  $key
	 * @param  mix $member 成员值
	 * @return mix [int|false] 存在返回int类型，不存在返回false
	 */
	public function zRank($key, $member) {
		$member = $this->_encode($member);
		$this->_initSlave();
		$res = $this->handle['slave']->zrank($key, $member);
		return $res;
	}
	
	/********************* about hashs *************************/
	
	/**
	 * 设置hash里面一个字段的值
	 * @param  string  $key
	 * @param  string $field hash中的字段名
	 * @param  mix $data field对应的值
	 * @return mix [false:出错|0:值已存在，被替换|1:添加成功，并且原先不存在这个值]
	 */
	public function hSet($key, $field, $data) {
		$data = $this->_encode($data);
		$this->_initMaters();
		$res = false;
		foreach ($this->handle['masters'] as $k => $v) {
			$res = $this->handle['masters'][$k]->hset($key, $field, $data);
		}
		return $res;
	}
	
	/**
	 * 设置hash字段值
	 * （同时添加多个字段和多个对应的值）
	 * @param  string  $key
	 * @param  array  $data 
	 * $data = array($field => $value, ......)
	 * @return int [1:成功|0:失败]
	 */
	public function hMSet($key, $data = array()) {
		if(empty($data) || !is_array($data)){
			return false;
		}
		foreach ($data as $k => $v) {
			$data[$k] = $this->_encode($v);
		}
		$this->_initMaters();
		$status = true;
		foreach ($this->handle['masters'] as $k => $v) {
			$status = $status & $this->handle['masters'][$k]->hmset($key, $data);
		}
		return $status;
	}
	
	/**
	 * 设置hash的一个字段，只有当这个字段不存在时有效
	 * @param  string  $key
	 * @param  array  $field hash中的字段名
	 * @param  array  $data field对应的值
	 * @return int [1:成功|0:失败]
	 */
	public function hSetNx($key, $field, $data) {
		$data = $this->_encode($data);
		$this->_initMaters();
		$status = true;
		foreach ($this->handle['masters'] as $k => $v) {
			$status = $status & $this->handle['masters'][$k]->hsetnx($key, $field, $data);
		}
		return $status;
	}
	
	/**
	 * 读取哈希里的一个字段的值
	 * @param  string  $key
	 * @param  string $field hash中的字段名
	 * @return mix [false失败|正确返回字段的值]
	 */
	public function hGet($key, $field) {
		$this->_initSlave();
		$res = $this->handle['slave']->hget($key, $field);
		if (false !== $res) {
			$res = $this->_decode($res);
		}
		return $res;
	}
	
	/**
	 * 获取hash里面指定字段的值
	 * （用来获取多个字段的值)
	 * @param  string  $key
	 * @param  array  $field hash中的字段名
	 * @return array
	 * array (
	 *	 'field1' => value1,
	 *	 ......)
	 */
	public function hMGet($key, $field = array()) {
		if(empty($field) || !is_array($field)){
			return array();
		}
		$this->_initSlave();
		$res = array();
		$res = $this->handle['slave']->hmget($key, $field);
		foreach ($res as $k => $v) {
			if (false !== $v) {
				$res[$k] = $this->_decode($v);
			}else{
				unset($res[$k]);
			}
		}
		return $res;
	}
	
	/**
	 * 获得hash的所有field与值
	 * @param  string  $key
	 * @return array
	 * array (
	 *	 'field1' => value1,
	 *	 ......)
	 */
	public function hGetAll($key) {
		$this->_initSlave();
		$res = $this->handle['slave']->hgetall($key);
		foreach($res as $k => $v) {
			$res[$k] = $this->_decode($v);
		}
		return $res;
	}
	
	/**
	 * 获得hash的所有field
	 * @param  string  $key
	 * @return array
	 * array (
	 *	 0 => field1,
	 *	 ......)
	 */
	public function hKeys($key) {
		$this->_initSlave();
		$res = $this->handle['slave']->hkeys($key);
		return $res;
	}
	
	/**
	 * 获得hash的所有值
	 * @param  string  $key
	 * @return array
	 * array (
	 *	 0 => value1,
	 *	 ......)
	 */
	public function hVals($key) {
		$this->_initSlave();
		$res = $this->handle['slave']->hvals($key);
		foreach($res as $k => $v) {
			$res[$k] = $this->_decode($v);
		}
		return $res;
	}
	
	/**
	 * 判断给定字段是否存在于哈希表中
	 * @param  string  $key
	 * @param  string $field hash中的字段名
	 * @return boolean [true存在|false不存在]
	 */
	public function hExists($key, $field) {
		$this->_initSlave();
		$res = $this->handle['slave']->hexists($key, $field);
		return $res;
	}
	
	/**
	 * 获取hash里所有字段的数量
	 * @param  string  $key
	 * @return mix [false:key存在，但不是hash类型|0:key不存在|正确返回>0的正整数]
	 */
	public function hLen($key) {
		$this->_initSlave();
		$res = $this->handle['slave']->hlen($key);
		return $res;
	}
	
	/**
	 * 删除一个或多个哈希字段
	 * 支持hDel key field [field ...]
	 * @param  string  $key
	 * @param  string $field hash中的字段名
	 * @return int 被删除了的字段个数
	 */
	public function hDel($key, $field) {
		$this->_initMaters();
		$args = func_get_args();
		$nums = 0;
		foreach ($this->handle['masters'] as $k => $v) {
			$nums = call_user_func_array(array($this->handle['masters'][$k], 'hdel'), $args);
		}
		return $nums;
	}
	
	/**
	 * 将哈希集中指定字段的值增加给定的数字（整数）
	 * @param  string  $key
	 * @param  array  $field hash中的字段名
	 * @param  integer $offset 偏移量（整数,可以是负数）
	 * @return mix [false:要操作的field的值不是整数|int:发生增减后field的值]
	 */
	public function hIncrBy($key, $field, $offset = 1) {
		$offset = (int)$offset;
		$this->_initMaters();
		$res = false;
		foreach ($this->handle['masters'] as $k => $v) {
			$res = $this->handle['masters'][$k]->hincrby($key, $field, $offset);
		}
		return $res;
	}
	
	/**
	 * 将哈希集中指定域的值增加给定的浮点数
	 * @param  string  $key
	 * @param  array  $field hash中的字段名
	 * @param  float  $offset
	 * @return mix [false:要操作的field的值不是整数或浮点数|float:发生增减后field的值]
	 */
	public function hIncrByFloat($key, $field, $offset = 1.0) {
		$offset = (float)$offset;
		$this->_initMaters();
		$res = false;
		foreach ($this->handle['masters'] as $k => $v) {
			$res = $this->handle['masters'][$k]->hincrbyfloat($key, $field, $offset);
		}
		return $res;
	}
	
	/********************* about list *************************/
	
	/**
	 * 将所有指定的值插入到存于 key 的列表的头部。
	 * 如果 key 不存在，那么在进行 push 操作前会创建一个空列表。 如果 key 对应的值不是一个 list 的话，那么会返回一个错误。
	 * 支持lPush key value [value ...]
	 * @param  string  $key
	 * @param  mix $data
	 * @return int 在 push 操作后的 list 长度。
	 */
	public function lPush($key, $data) {
		$args = func_get_args();
		$this->_initMaters();
		$res = false;
		foreach($args as $k => $v){
			$args[$k] = $k > 0 ? $this->_encode($v) : $v;
		}
		foreach ($this->handle['masters'] as $k => $v) {
			$res = call_user_func_array(array($this->handle['masters'][$k], 'lpush'), $args);
		}
		return $res;
	}
	
	/**
	 * 将所有指定的值插入到存于 key 的列表的尾部。
	 * 如果 key 不存在，那么在进行 push 操作前会创建一个空列表。 如果 key 对应的值不是一个 list 的话，那么会返回一个错误。
	 * 支持rPush key value [value ...]
	 * @param  string  $key
	 * @param  mix $data
	 * @return int 在 push 操作后的 list 长度。
	 */
	public function rPush($key, $data) {
		$args = func_get_args();
		$this->_initMaters();
		$res = false;
		foreach($args as $k => $v){
			$args[$k] = $k > 0 ? $this->_encode($v) : $v;
		}
		foreach ($this->handle['masters'] as $k => $v) {
			$res = call_user_func_array(array($this->handle['masters'][$k], 'rpush'), $args);
		}
		return $res;
	}
	
	/**
	 * 从列表中获取指定返回的元素
	 * @param  string  $key
	 * @param  int $start
	 * @param  int $end
	 * @return array
	 */
	public function lRange($key, $start = 0, $end = -1) {
		$this->_initSlave();
		$res = $this->handle['slave']->lrange($key, $start, $end);
		if (true === is_array($res)) {
			foreach ($res as $k => $v) {
				$res[$k] = $this->_decode($v);
			}
		}
		return $res;
	}
	
	/**
	 * 弹出 key 对应的 list 的第一个元素。
	 * @param  string  $key
	 * @return mix [false空队列]
	 */
	public function lPop($key) {
		$this->_initMaters();
		$res = false;
		foreach ($this->handle['masters'] as $k => $v) {
			$res =  $this->handle['masters'][$k]->lpop($key);
		}
		if (false !== $res) {
			$res = $this->_decode($res);
		}
		return $res;
	}
	
	/**
	 * 弹出 key 对应的 list 的最后一个元素。
	 * @param  string  $key
	 * @return mix [false空队列]
	 */
	public function rPop($key) {
		$this->_initMaters();
		$res = false;
		foreach ($this->handle['masters'] as $k => $v) {
			$res =  $this->handle['masters'][$k]->rpop($key);
		}
		if (false !== $res) {
			$res = $this->_decode($res);
		}
		return $res;
	}
	
	/**
	 * 从存于 key 的列表里移除前 count 次出现的值为 value 的元素。
	 * count > 0: 从头往尾移除值为 value 的元素。
	 * count < 0: 从尾往头移除值为 value 的元素。
	 * count = 0: 移除所有值为 value 的元素。
	 * @param  string  $key
	 * @param mix $data 特别注意数字是否有引号，因为这里是会做json_encode的操作，当非int类型数字时，是会有引号出现的。
	 * @return int 被移除的元素个数。
	 */
	public function lRem($key, $data, $count = 1) {
		$this->_initMaters();
		$res = 0;
		foreach ($this->handle['masters'] as $k => $v) {
			$res = $this->handle['masters'][$k]->lrem($key, $data, $count);
		}
		return $res;
	}


}
