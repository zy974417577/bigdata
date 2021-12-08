package com.sheep.flink.exercise03.watemake;

import com.sheep.flink.exercise02.bean.Persion;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

public class MyWatermarkGeneratorSupplier implements WatermarkGenerator<Persion> {

    private final long maxOutOfOrderness = 5000L; // 3.5 秒

    private long currentMaxTimestamp;

    /**
     * 每来一条事件数据调用一次，可以检查或者记录事件的时间戳，或者也可以基于事件数据本身去生成 watermark。
     */

    @Override
    public void onEvent(Persion event, long eventTimestamp, WatermarkOutput output) {
        currentMaxTimestamp = Math.max(currentMaxTimestamp,Long.valueOf(event.getBirthday()));
    }

    /**
     * 周期性的调用，也许会生成新的 watermark，也许不会。
     *
     * <p>调用此方法生成 watermark 的间隔时间由 {ExecutionConfig#getAutoWatermarkInterval()} 决定。
     */

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
//        output.emitWatermark(new Watermark(System.currentTimeMillis()-5000L));
        output.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness - 1));
    }
}
