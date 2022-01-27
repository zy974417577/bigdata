package com.sheep.project.etl.core;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 * 双流join,根据redis中的数据匹配业务码表中的数据
 */
public class NationJoinCodeTaleProcess extends CoProcessFunction<String, HashMap<String, String>, String> {

    //TODO 用来获取广播变量中的数据
    private HashMap broadcastMap = new HashMap<String, String>();

    @Override
    public void processElement1(String value, Context ctx, Collector<String> out) throws Exception {
        JSONObject jsonOrderFrom = JSONObject.parseObject(value);
        String countryCode = jsonOrderFrom.getString("countryCode");
        String dt = jsonOrderFrom.getString("dt");
        String area = (String) broadcastMap.get(countryCode);
        JSONArray jsonArray = jsonOrderFrom.getJSONArray("orderData");

        for (int i = 0; i < jsonArray.size(); i++) {
            JSONObject dataObject = jsonArray.getJSONObject(i);
            dataObject.put("dt", dt);
            dataObject.put("area", area);
            //下游获取到数据的时候，也就是一个json格式的数据
            out.collect(dataObject.toJSONString());
        }
    }


    @Override
    public void processElement2(HashMap<String, String> value, Context ctx, Collector<String> out) throws Exception {
        //获取map
        broadcastMap = value;
    }
}
