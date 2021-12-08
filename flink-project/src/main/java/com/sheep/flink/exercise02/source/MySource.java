package com.sheep.flink.exercise02.source;

import com.sheep.flink.exercise02.bean.Persion;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class MySource extends RichParallelSourceFunction<Persion> {
    public final Logger logs = LoggerFactory.getLogger(MySource.class);

    @Override
    public void run(SourceContext<Persion> ctx) throws Exception {

        String current = String.valueOf(System.currentTimeMillis());
        FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");

        while (Integer.valueOf(current.substring(current.length() - 4)) > 100) {
            current = String.valueOf(System.currentTimeMillis());
            continue;
        }

        System.out.println("当前时间: "+ dateFormat.format(System.currentTimeMillis()));



        TimeUnit.SECONDS.sleep(13);
        Persion persion01 = new Persion("hadoop", 1, "" + System.currentTimeMillis());
//        logs.warn("persion01: {}",persion01);
//        ctx.collect(persion01);

        TimeUnit.SECONDS.sleep(2);
        Persion persion02 = new Persion("hadoop", 1, "" + System.currentTimeMillis());
//        logs.warn("persion02: {}",persion02);
        ctx.collect(persion02);

        TimeUnit.SECONDS.sleep(3);
        Persion persion03 = new Persion("hadoop", 1, "" + System.currentTimeMillis());
//        logs.warn("persion03: {}",persion03);
        ctx.collect(persion03);

        ctx.collect(persion01);

        TimeUnit.SECONDS.sleep(3000000);

    }

    @Override
    public void cancel() {

    }
}
