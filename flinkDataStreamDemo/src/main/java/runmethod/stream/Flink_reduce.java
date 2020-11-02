package runmethod.stream;

import model.Student;
import mysource.MyDemoSource;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * reduce:压缩操作，可以把一个窗口内的数据累计在某一个流里，做一个比喻的话，A班和B班要比小红花的数量，则先keyby两个班的学生再reduce所有小红花在班长一个的的手里，最后比较
 * @author ：why
 * @description：TODO
 * @date ：2020/9/28 22:34
 */


public class Flink_reduce {

    public static void main(String[] args) {
        //获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Student> dataStream = env.addSource(new MyDemoSource());
        SingleOutputStreamOperator<Student> reduce = dataStream.keyBy("subject")
                .timeWindow(Time.seconds(5))
                .reduce(new ReduceFunction<Student>() {
                    @Override
                    public Student reduce(Student student, Student t1) {
                        student.setTuition(student.getTuition() + t1.getTuition());
                        student.setScore(student.getScore() + t1.getScore());
                        student.setSubject(student.getSubject() + "---" + t1.getSubject());
                        return student;
                    }
                });
        reduce.print().setParallelism(1);
    }

}
