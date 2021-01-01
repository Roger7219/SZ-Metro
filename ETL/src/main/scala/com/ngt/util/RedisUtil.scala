package com.ngt.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * @author ngt
 * @create 2020-12-23 21:48
 */
object RedisUtil {
  private val jedisPoolConfig: JedisPoolConfig = new JedisPoolConfig()
  jedisPoolConfig.setMaxTotal(200) //最大连接数
  jedisPoolConfig.setMaxIdle(20) //连接池中最大空闲的连接数
  jedisPoolConfig.setMinIdle(20) //最小空闲
  jedisPoolConfig.setBlockWhenExhausted(true) //忙碌时是否等待
  jedisPoolConfig.setMaxWaitMillis(2000) //忙碌时等待时长 毫秒
  jedisPoolConfig.setTestOnBorrow(false) //每次获得连接的进行测试

  private val jedisPool: JedisPool = new JedisPool(jedisPoolConfig, "192.168.31.8", 6379)

  def getJedisClient: Jedis = {
    jedisPool.getResource
  }

  //  redis 集群
  //  private val nodes: mutable.Set[HostAndPort] = mutable.Set[HostAndPort]()
  //	nodes.add(new HostAndPort("192.168.31.8",7001))
  //	nodes.add(new HostAndPort("192.168.31.8",7002))
  //	nodes.add(new HostAndPort("192.168.31.8",7003))
  //	nodes.add(new HostAndPort("192.168.31.8",7004))
  //	nodes.add(new HostAndPort("192.168.31.8",7005))
  //	nodes.add(new HostAndPort("192.168.31.8",7006))
  //  new JedisPool(jedisPoolConfig, nodes)

}
