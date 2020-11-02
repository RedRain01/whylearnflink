package runmethod.stream;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import model.Student;
import mysource.MyDemoSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Iterator;
import java.util.Set;

/**
 * map：在flink中用于一个流对另一个流的转换，比如把一个dataStream<Strudent>-转成dataStream<String>
 *
 * @author ：why
 * @description：TODO
 * @date ：2020/9/28 22:34
 */


public class Flink_richmap {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Student> dataStream = env.addSource(new MyDemoSource());

        //定义执行环境

        //获取Flink的运行环境
        //获取自定义数据源
        dataStream.map(new RichMapFunction<Student, Object>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }
            @Override
            public Object map(Student value) throws Exception {
                return null;
            }
        });
        SingleOutputStreamOperator<String> map = dataStream.map(new MapFunction<Student, String>() {
            @Override
            public String map(Student student) {
                int i = 0;
                String next = "";
                JSONObject obj = JSONObject.parseObject(JSON.toJSONString(student));
                Set<String> strings = obj.keySet();
                Iterator<String> iterator = strings.iterator();
                while (iterator.hasNext()) {
                    i++;
                    next = iterator.next();
                }
                return next;
            }
        });
        map.print().setParallelism(1);
        String jobName = Flink_richmap.class.getSimpleName();
        env.execute(jobName);
    }

}
