package com.utils;


import org.junit.Test;
import redis.clients.jedis.Jedis;

public class RedisTest {

    @Test
    public void redisConnectorTest(){

        String host = "192.168.72.53";
        int port = 6379;



        Jedis jedisConnector =  RedisConnector.getJedisPoolConfigConnector(host,port);

//        jedisConnector.hset("key1","f1","v1");

        String value = jedisConnector.hget("key1","f1");

        System.out.println(value);

        jedisConnector.close();
    }

}
