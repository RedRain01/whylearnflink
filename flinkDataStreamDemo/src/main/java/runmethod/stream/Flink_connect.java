package runmethod.stream;

import com.alibaba.fastjson.JSON;
import model.Student;
import mysource.MyDemoSource;
import mysource.MyDemoSource2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * 合并流：connect union
 * connect：只可以合并两个流，结果是ConnectedStreams，但是这两个流可以类型不同
 * union：可以合并多个流，但是必须是类型相同的流，结果是dataStream
 *
 * @author ：why
 * @description：TODO
 * @date ：2020/9/28 22:34
 */


public class Flink_connect {

    public static void main(String[] args) {
        //获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Student> dataStream = env.addSource(new MyDemoSource());
        DataStream<Student> dataStream2 = env.addSource(new MyDemoSource2());

        //ConnectedStreams并没有继承DataStream类，所以要对ConnectedStreams进行操作必须进行comap操作
        ConnectedStreams<Student, Student> connect = dataStream.connect(dataStream2);
        //CoMapFunction 和map 几乎一样，唯一的不同就是 CoMapFunction里是对两个流做map
        SingleOutputStreamOperator<Student> map1 = connect.map(new CoMapFunction<Student, Student, Student>() {
            @Override
            public Student map1(Student value) throws Exception {
                System.out.println("--11-----------" + JSON.toJSONString(value));
                return value;
            }

            @Override
            public Student map2(Student value) throws Exception {
                System.out.println("-22------------" + JSON.toJSONString(value));
                return value;
            }
        });

        /**
         * union:要数据类型相同的流
         */
        DataStream<Student> union = dataStream.union(dataStream2, dataStream);

    }

}
