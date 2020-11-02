package runmethod.window;

/**
 * @author ：why
 * @description：TODO
 * @date ：2020/10/16 11:17
 */



import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @author qingh.yxb
 * @since 2019/8/10
 */
public class MyTrigger extends Trigger<Tuple2<String, Integer>, TimeWindow> {
    private static final long serialVersionUID = 1L;
    ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("total", Integer.class);

    @Override
    public TriggerResult onElement(Tuple2<String, Integer> element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        ValueState<Integer> sumState = ctx.getPartitionedState(stateDescriptor);
        if (null == sumState.value()) {
            sumState.update(0);
        }
        sumState.update(element.f1 + sumState.value());
        if (sumState.value() >= 2) {
            //这里可以选择手动处理状态
            //  默认的trigger发送是TriggerResult.FIRE 不会清除窗口数据
            return TriggerResult.FIRE_AND_PURGE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        System.out.println("清理窗口状态  窗口内保存值为" + ctx.getPartitionedState(stateDescriptor).value());
        ctx.getPartitionedState(stateDescriptor).clear();
    }
}