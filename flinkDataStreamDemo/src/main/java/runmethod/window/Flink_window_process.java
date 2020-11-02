package runmethod.window;

import model.Student;
import mysource.MyDemoSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

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


public class Flink_window_process {

    public static void main(String[] args) throws Exception {
        //获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Student> dataStream = env.addSource(new MyDemoSource());
        //指定时间窗口大小为5秒，

        SingleOutputStreamOperator<Student> subject = dataStream.keyBy("score")
                .timeWindow(Time.seconds(5))
                .process(new ProcessWindowFunction<Student, Student, Tuple, TimeWindow>() {
                    int  score=0;
                    @Override
                    public void process(Tuple tuple, Context context, Iterable<Student> elements, Collector<Student> aggregate) throws Exception {
                        System.out.println("---tuple-----"+tuple+"-------------currentWatermark--"+context.currentWatermark()+"----------");
                        for (Student student:elements  ) {
                           score=student.getScore()+score;
                        }
                        Student student = new Student();
                        student.setScore(score);
                        aggregate.collect(student);
                    }
                });
        subject.print().setParallelism(1);
        env.execute("0000000000098");

    }

}
