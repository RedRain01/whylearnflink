package runmethod.window;

/**
 * @author ：why
 * @description：TODO
 * @date ：2020/10/16 11:17
 */

//        if (sumState.value() >= 2) {
//            System.out.println("---112-------------------"+sumState.value());
//            //这里可以选择手动处理状态
//            //  默认的trigger发送是TriggerResult.FIRE 不会清除窗口数据
//            /***
//             * TriggerResult中包含四个枚举值：
//             *
//             *     CONTINUE 表示对窗口不执行任何操作。
//             *     FIRE 表示对窗口中的数据按照窗口函数中的逻辑进行计算，并将结果输出。注意计算完成后，窗口中的数据并不会被清除，将会被保留。
//             *     PURGE 表示将窗口中的数据和窗口清除。All elements in the window are cleared and the window is discarded, without evaluating the window function or emitting any elements.
//             *     FIRE_AND_PURGE 表示先将数据进行计算，输出结果，然后将窗口中的数据和窗口进行清除。
//             */
//            return TriggerResult.FIRE_AND_PURGE;
//        }

import model.Student;
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
public class MyTrigger001 extends Trigger<Student, TimeWindow> {
    private static final long serialVersionUID = 1L;
    ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("total", Integer.class);

    @Override
    public TriggerResult onElement(Student element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
       ValueState<Integer> sumState = ctx.getPartitionedState(stateDescriptor);
        if (null == sumState.value()) {
            sumState.update(0);
        }
         sumState.update(element.getScore() + sumState.value());
       // System.out.println("------55555555-------------------------"+sumState.value());
        //      return TriggerResult.FIRE;
        //     return TriggerResult.CONTINUE;
        //   return TriggerResult.PURGE;
//       if (sumState.value()==10) {
//           return  TriggerResult.FIRE;
//        }
      return TriggerResult.FIRE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        System.out.println("------------1111111111111------------------------------------");
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        System.out.println("-------------22222222222-------------------------------");

        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        System.out.println("---------清理窗口状态  窗口内保存值为" + ctx.getPartitionedState(stateDescriptor).value());
      //  ctx.getPartitionedState(stateDescriptor).clear();

    }
}