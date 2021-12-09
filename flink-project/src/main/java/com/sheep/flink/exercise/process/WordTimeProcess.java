package com.sheep.flink.exercise.process;

import com.sheep.flink.exercise.bean.WordTime;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WordTimeProcess extends ProcessWindowFunction<WordTime, Tuple2<String, Integer>, String, TimeWindow> {

    @Override
    public void process(String key, Context context, Iterable<WordTime> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
        FastDateFormat dateFormat = FastDateFormat.getInstance("yyyyMMdd HH:mm:ss");
        String startFormat = dateFormat.format(context.window().getStart());
        String endFormat = dateFormat.format(context.window().getEnd());

        System.out.println("窗口开始时间： " + startFormat);
        System.out.println("窗口结束时间： " + endFormat);
        int i = 0;
        for (WordTime element : elements) {
            i++;
        }
        out.collect(Tuple2.of(key, i));
    }
}
