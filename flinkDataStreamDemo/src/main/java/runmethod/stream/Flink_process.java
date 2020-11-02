package runmethod.stream;

import model.Student2;
import model.StudentScoreMonitor;
import mysource.MyDemoSourceMonitor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 定时器：在监控某个事件上，用窗口实现比较困难，但是使用processAPI实现就比较方便,process是比较基础的api可以过去到window的时长 和watermark
 * @author ：why
 * @description：TODO
 * @date ：2020/9/28 22:34
 */


public class Flink_process {

    /**
     * 目的：监控学生成绩，当学生成绩连续下降的时候，发出警报
     * 思路1：在一个时间窗口内，每次把窗口中的数据进行比较，比较连续上升的如果有反弹，舍弃该窗口，立即开始下一个窗口，以此类推
     * 思路一实现：放弃窗口目前不知道怎么做到，如果不放弃窗口就无法判断前窗口的后半段和后窗口的前半段是否构成连续下降，所以暂时放弃该做法，
     * 思路二：使用定时器，每次成绩和上一次做对比，如果成绩大于上一次就更新一次定时器，小于不更新定时器，这样定时器不更新到截至时间就触发方法，那如果只出现一次是不是也会触发定时器，可以在触方法里做对比
     * @param args
     */
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Student2> studentDataStreamSource = env.addSource(new MyDemoSourceMonitor());
        SingleOutputStreamOperator<String> process = studentDataStreamSource.keyBy("name")
                .process(new KeyedProcessFunction<Tuple, Student2, String>() {

                    // 自定义状态
                    private ValueState<StudentScoreMonitor> state;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 初始化状态，name是myState
                        state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", StudentScoreMonitor.class));
                    }

                    @Override
                    public void processElement(Student2 value, Context ctx, Collector<String> out) throws Exception {
                        int score = value.getScore();
                        StudentScoreMonitor value1 = state.value();
                        Long timestamp = ctx.timerService().currentProcessingTime()+1000L;
                        if (value1 == null) {
                            value1 = new StudentScoreMonitor();
                            value1.setName(value.getName());
                            value1.setTime(value.getTime());
                            value1.setScore(value.getScore());
                          //  value1.setLastModified(System.currentTimeMillis());
                            value1.setLastModified(ctx.timerService().currentProcessingTime()+1000L);
                            state.update(value1);
                        } else if (value1.getScore() < (long) score) {
                            value1.setLastModified(ctx.timerService().currentProcessingTime()+1000L);
                          //  value1.setLastModified(System.currentTimeMillis());
                            value1.setScore(value.getScore());
                            state.update(value1);
                        }else {
                            value1.setScore(value.getScore());
                        }
                        // 为当前单词创建定时器，十秒后后触发
                        long timer = value1.getLastModified() + 10000;
                        ctx.timerService().registerProcessingTimeTimer(timer);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        // 取得当前单词
                        Tuple currentKey = ctx.getCurrentKey();
                        // 取得该单词的myState状态
                        StudentScoreMonitor result = state.value();
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        String time_Date = sdf.format(new Date(result.getTime()));
                        out.collect("出现成绩下滑学生：" + result.getName() + "成绩：" + result.getScore() + "上次考试考试时间" + time_Date);
                    }
                });
        process.print().setParallelism(1);
        String jobName = Flink_process.class.getSimpleName();
        env.execute(jobName);
    }

}
