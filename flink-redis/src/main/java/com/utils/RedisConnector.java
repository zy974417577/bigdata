package com.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * 获取Redis连接
 */
public class RedisConnector {
    private static Jedis jedis;
    private static JedisPool jedisPool;
    private static JedisPoolConfig jedisPoolConfig;

    /**
     * 获取Redis连接池
     * @param host ip地址
     * @param port 端口号
     * @return
     */
    static public Jedis getJedisPoolConfigConnector(String host,int port){
        jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(10);
        jedisPool = new JedisPool(jedisPoolConfig, host, port);
        jedis = jedisPool.getResource();
        return jedis;
    }

    public Jedis getStandAloneRedisConnector(String host,int port){

        jedis = new Jedis(host, port);

        return jedis;
    }

    public void closeStandAloneRedisConnector(){
        if(null != jedis) jedis.close();
    }
}
