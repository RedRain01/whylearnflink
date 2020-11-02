package runmethod.stream;

import model.Student;
import mysource.MyDemoSource;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * split可以根据条件分割流，再按照之前放入的key再取出
 * @author ：why
 * @description：TODO
 * @date ：2020/9/28 22:34
 */


public class Flink_split {

    public static void main(String[] args) {
        //获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Student> dataStream = env.addSource(new MyDemoSource());

        SplitStream<Student> split = dataStream.split(new OutputSelector<Student>() {
            @Override
            public Iterable<String> select(Student student) {
                ArrayList<String> outPut = new ArrayList<>();
                if ("数学".equals(student.getSubject())) {
                    outPut.add("数学");
                } else {
                    outPut.add("非数学");
                }
                return outPut;
            }

        });
        DataStream<Student> select = split.select("数学");
        DataStream<Student> select2 = split.select("非数学");

    }

}
