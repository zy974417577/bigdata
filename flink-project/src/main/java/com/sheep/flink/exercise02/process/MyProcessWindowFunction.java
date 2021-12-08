package com.sheep.flink.exercise02.process;

import com.sheep.flink.exercise02.FlinkWateMake;
import com.sheep.flink.exercise02.bean.Persion;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//IN, OUT, KEY, W
public class MyProcessWindowFunction extends ProcessWindowFunction<Persion, Tuple2<String,Integer>, String, TimeWindow> {
    public static final Logger logs =  LoggerFactory.getLogger(FlinkWateMake.class);

    @Override
    public void process(String value, Context context, Iterable<Persion> elements, Collector<Tuple2<String, Integer>> out) throws Exception {


        FastDateFormat format = FastDateFormat.getInstance("yyyy/MM/dd HH:mm:ss");

        //TODO 获取窗口开始时间
        long start = context.window().getStart();
        String startTime = format.format(start);
        logs.warn("窗口开始时间: {}",startTime);
        //TODO 获取窗口结束时间
        long end = context.window().getEnd();
        String endTime = format.format(end);
        logs.warn("窗口结束时间: {}",endTime);

        int number = 0;

        for (Persion persion : elements) {
            number+=1;
        }

        out.collect(Tuple2.of(value,number));
    }
}
