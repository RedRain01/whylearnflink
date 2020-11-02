package otherDemo;

/**
 * @author ：why
 * @description：TODO
 * @date ：2020/10/27 19:38
 */

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;


public class DayTrigger<JSONObject> extends Trigger<com.alibaba.fastjson.JSONObject, Window> {
    private ReducingStateDescriptor<Long> countStateDescriptor =
            new ReducingStateDescriptor("counter", new Sum(), LongSerializer.INSTANCE);

    ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("total", Integer.class);

    //每个元素都会触发
    @Override
    public TriggerResult onElement(com.alibaba.fastjson.JSONObject element, long timestamp, Window window, TriggerContext ctx) throws Exception {
        System.out.println("先打印timestamp = " + timestamp+",本地时间："+System.currentTimeMillis());
        ValueState<Integer> sumState = ctx.getPartitionedState(stateDescriptor);
        if (null == sumState.value()) {
            sumState.update(0);
        }

        sumState.update(1 + sumState.value());
        if (sumState.value() >= 2) {
            //这里可以选择手动处理状态
            //  默认的trigger发送是TriggerResult.FIRE 不会清除窗口数据
            System.out.println("触发.....数据条数为：" + (1 + sumState.value()));
            return TriggerResult.FIRE;
        }
        Long todayZeroPointTimestamps = getTodayZeroPointTimestamps();
        System.out.println("todayZeroPointTimestamps = " + todayZeroPointTimestamps);
        if (timestamp >= todayZeroPointTimestamps) {
            return TriggerResult.FIRE_AND_PURGE;
        }
        if (timestamp >= window.maxTimestamp()) {
            return TriggerResult.FIRE_AND_PURGE;

        } else {
            return TriggerResult.CONTINUE;
        }


//        ReducingState<Long> countState = ctx.getPartitionedState(countStateDescriptor);
//        countState.add(1L);
//
  //      System.out.println("打印countState.get() = " + countState.get());
//        if (countState.get() >= 3) {
//            System.out.println("触发了............. " );
//             //这里是触发并且清除
//            return TriggerResult.FIRE_AND_PURGE;
//        }
//        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, Window window, TriggerContext triggerContext) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, Window window, TriggerContext triggerContext) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(Window window, TriggerContext ctx) throws Exception {
//        System.out.println("清理窗口状态  窗口内保存值为" + ctx.getPartitionedState(stateDescriptor).value());
        ctx.getPartitionedState(stateDescriptor).clear();

    }

    class Sum implements ReduceFunction<Long> {

        @Override
        public Long reduce(Long value1, Long value2) throws Exception {
            return value1 + value2;
        }
    }

    public static Long getTodayZeroPointTimestamps() {

        long now = System.currentTimeMillis();
        long daySecond = 60 * 60 * 24 * 1000;
        long dayTime = now - (now + 8 * 3600 * 1000) % daySecond + 1 * daySecond;
        return dayTime;

    }

    public static void main(String[] args) {
        Long todayZeroPointTimestamps = getTodayZeroPointTimestamps();
        System.out.println("todayZeroPointTimestamps = " + todayZeroPointTimestamps);

    }

}