package runmethod.stream;

import model.Student;
import mysource.MyDemoSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ：why
 * @description：TODO
 * @date ：2020/9/28 22:34
 */


public class Flink_union {

    public static void main(String[] args) {
        //获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Student> dataStream = env.addSource(new MyDemoSource());

    }

}
