package runmethod.window;

import model.Student;
import mysource.MyDemoSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * window：理解为分批处理部分数据，种类有 Time Window Count Window Session Window
 * 时间窗口就是固定某段时间作为一个批次数据
 * cout窗口就是根据数据个数作为一个批次数据
 * session窗口根据数据活跃程度作为划分批次数据
 * 翻滚窗口：有序无交叉窗口
 * 滑动窗口：有序交叉窗口
 * @author ：why
 * @description：TODO
 * @date ：2020/9/28 22:34
 */


public class Flink_window_trigger {

    public static void main(String[] args) {
        //获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Student> dataStream = env.addSource(new MyDemoSource());
        //指定时间窗口大小为5秒，
        SingleOutputStreamOperator<Student> sum = dataStream.keyBy("subject")
                .timeWindow(Time.seconds(5))
                .trigger(new Trigger<Student, TimeWindow>() {
                    @Override
                    public TriggerResult onElement(Student element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
                        System.out.println("---element--"+element+"-------timestamp--"+timestamp+"--------window-"+window+"--------------------------");

                        return null;
                    }

                    @Override
                    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        return null;
                    }

                    @Override
                    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        return null;
                    }

                    @Override
                    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

                    }
                })
                .sum("score");
        //count窗口
        dataStream .keyBy("subject")
                .countWindow(5)
                .sum("score");
        //time滑动窗口
        dataStream.keyBy("subject")
                .timeWindow(Time.seconds(1),Time.seconds(2))
        .sum("score");
    }

}
