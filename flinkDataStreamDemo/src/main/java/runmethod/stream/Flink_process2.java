package runmethod.stream;

import model.Student;
import mysource.MyDemoSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * processApi 旁路输出，获取window时间
 *
 * @author ：why
 * @description：TODO
 * @date ：2020/9/28 22:34
 */


public class Flink_process2 {
    public static void main(String[] args) throws Exception {
        //获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Student> dataStream = env.addSource(new MyDemoSource());
        //IN, OUT, KEY, W extends Window
        /**
         * windowStream对于process的使用，通过参数可以看出int表示输入数据类型，out输出，key我认为是keyby的字段，window是你使用的窗口类型
         */
        SingleOutputStreamOperator<Tuple4<Long,Long,Integer,String>> sum1 = dataStream.keyBy("subject")
                .timeWindow(Time.seconds(5))
                .process(new ProcessWindowFunction<Student,Tuple4<Long,Long,Integer,String>, Tuple, TimeWindow>(){
                    Tuple4 tuple4=new Tuple4();
                    int scoreall=0;
                    @Override
                    public void process(Tuple tuple, Context context, Iterable<Student> elements, Collector<Tuple4<Long,Long,Integer,String>> out) throws Exception {
                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        tuple4.f0=start;
                        tuple4.f1=end;
                        for (Student tuple2 : elements) {
                            scoreall+=tuple2.getScore();
                        }
                        tuple4.f2=scoreall;
                        out.collect(tuple4);
                    }
                });
        /**
         *旁路输出(Side Outputs)
         * dataStream使用process，可以产生两个自定类型的流，一个作为主流方法的返回值，一个作为旁类流可以从主流中检出
         */
        // 定义OutputTag
        final OutputTag<Student> outputTag = new OutputTag<Student>("side-output"){};
        SingleOutputStreamOperator<String> process = dataStream.process(new ProcessFunction<Student, String>() {
            @Override
            public void processElement(Student value, Context ctx, Collector<String> out) throws Exception {
                out.collect(value.getSubject());
                ctx.output(outputTag, value);
            }
        });
        // 取得旁路数据
        DataStream<Student> sideDataStream = process.getSideOutput(outputTag);
        process.print();
        sideDataStream.print();
        //  sum1.print().setParallelism(1);
        String jobName = Flink_process2.class.getSimpleName();
        env.execute(jobName);
    }

}
