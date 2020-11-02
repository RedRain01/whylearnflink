package runmethod.stream;

import model.Student;
import mysource.MyDemoSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * filter:根据条件过滤dataStream中符合条件的流
 * @author ：why
 * @description：TODO
 * @date ：2020/9/28 22:34
 */


public class Flink_file {

    public static void main(String[] args) throws Exception {
        //获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Student> dataStream = env.addSource(new MyDemoSource());
        SingleOutputStreamOperator<Student> filter = dataStream.filter(new FilterFunction<Student>() {
            @Override
            public boolean filter(Student student) {
                return student.getSubject().equals("数学");
            }
        });
        filter.print().setParallelism(1);
        String jobName = Flink_file.class.getSimpleName();
        env.execute(jobName);

    }

}
