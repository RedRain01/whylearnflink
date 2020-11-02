package runmethod.stream;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import model.Student;
import mysource.MyDemoSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Set;

import static org.apache.flink.api.common.typeinfo.Types.*;
import static org.apache.flink.api.common.typeinfo.Types.STRING;

/**
 * @author ：flatmap 和map相似，都是流转换流，但不同的是flatmap可以将一条流分解成多条Datastream流
 * 我在理论上也一直解决map可以完全代替flatmap，不过等到实际操作的时候我就发现flatmap和map不同性是很大的，例如人这个流进入map处理，
 * 只能输出一条流出去，但是flatmap 可以解析人这个流分离出头发流 耳朵流。。。。
 * @description：TODO
 * @date ：2020/9/28 22:34
 */


public class Flink_flatmap {

    public static void main(String[] args) throws Exception {
        //获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Student> dataStream = env.addSource(new MyDemoSource());
//        dataStream.map((Student stu,String str)->{
//
//        } )
/**
 * Lambda 表达式 使用
 */
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = dataStream.flatMap( (Student line, Collector<Tuple2<String, Integer>> out) -> {
                out.collect(new Tuple2(line.getSubject(),1));
        }).returns(Types.TUPLE(Types.STRING, Types.INT));
        result.print().setParallelism(1);

        SingleOutputStreamOperator<String> stringSingleOutputStreamOperator = dataStream.flatMap(new FlatMapFunction<Student, String>() {
            @Override
            public void flatMap(Student student, Collector<String> collector) {
                String next = "";
                int i = 0;
                JSONObject obj = JSONObject.parseObject(JSON.toJSONString(student));
                Set<String> strings = obj.keySet();
                Iterator<String> iterator = strings.iterator();
                while (iterator.hasNext()) {
                    i++;
                    next = iterator.next();
                    collector.collect(next);
                }
            }
        });
        //stringSingleOutputStreamOperator.print().setParallelism(1);
        env.execute("testflatma222p");
    }

}
