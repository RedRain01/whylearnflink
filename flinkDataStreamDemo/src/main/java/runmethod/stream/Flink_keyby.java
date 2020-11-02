package runmethod.stream;

import model.Student;
import mysource.MyDemoSource;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * keyBy：分组，dataStream分组后返回KeyedStream,例如把一个学校的 DataStream<Student> 按班级分配到下游算子，keyby就是班级
 * keyby可用于解决某种类型的算子乱序问题，效果等同kafka分区解决乱序问题，将不同类型数据keyby将解决某种业务上的乱序问题
 * @author ：why
 * @description：TODO
 * @date ：2020/9/28 22:34
 */


public class Flink_keyby {

    public static void main(String[] args) throws Exception {
        //获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Student> dataStream = env.addSource(new MyDemoSource());
        KeySelector keySelector = new KeySelector<Student, String>() {
            @Override
            public String getKey(Student value) throws Exception {
                return value.getClazz();
            }
        };
        SingleOutputStreamOperator score = dataStream.keyBy(keySelector)
                .timeWindow(Time.seconds(5))
                .sum("score");
        score.print().setParallelism(1);
        env.execute("finkkeyby");
        //指定时间窗口大小为2秒，指定时间间隔为1秒
        dataStream.keyBy(1);
        SingleOutputStreamOperator<Student> sum = dataStream.keyBy("subject")
                .timeWindow(Time.seconds(5))
                .sum("score");

    }
}
