package com.sheep.project.etl.source;

import com.utils.RedisConnector;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

/**
 * 读取Redis中码值表
 */
public class RedisNationSource extends RichParallelSourceFunction<HashMap<String, String>> {

    private final Logger logs = LoggerFactory.getLogger(RedisNationSource.class);
    private boolean isRunning=true;
    private Jedis jedis;

    /**
     * 初始化链接
     */
    private void init() {
        try {
            jedis =  RedisConnector.getJedisPoolConfigConnector("cdh003", 6379);
        }catch (Exception e){
            logs.error("Redis初始化失败!!!");
        }
    }

    /**
     * 获取Redis中码表数据
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<HashMap<String, String>> ctx) throws Exception {
        init();
        HashMap<String, String> map = new HashMap<>();
        while (isRunning){
            try {
                map.clear();
                Map<String, String> countryRedis = jedis.hgetAll("country");
                for (Map.Entry<String, String> country : countryRedis.entrySet()) {
                    String key = country.getKey();
                    String value = country.getValue();
                    String[] codes = value.split(",");
                    for (String code : codes) {
                        map.put(code, key);
                    }
                    if (map.size() > 0) {
                        ctx.collect(map);
                    }
                }
            }catch ( Exception e){
                logs.error("redis获取码表失败",e.getCause());
            }
        }
    }

    /**
     * 归还Redis链接
     */
    @Override
    public void cancel() {
        isRunning = false;
        if(null != jedis){
            jedis.close();
        }

    }
}
