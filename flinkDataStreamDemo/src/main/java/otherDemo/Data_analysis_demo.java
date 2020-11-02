package otherDemo;

/**
 * @author ：why
 * @description：TODO
 * @date ：2020/10/27 19:33
 */


import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * todo 数据分析
 * todo http://docs-aliyun.cn-hangzhou.oss.aliyun-inc.com/assets/pic/49939/cn_zh/1487929553566/man-screen-shot.jpg
 *
 * todo 指标维度： 1，活跃用户  2，新增用户 3，登陆会员 4，新注册用户 5，启动次数
 *
 * todo 思路1： 每秒活跃用户数量都要存储到hbase，使用窗口函数，窗口大小为1s
 * todo 思路2：每天凌晨状态清零，开始重新累加计算，所以需要
 *
 */
public class Data_analysis_demo {
    public static void main(String[] args) {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //todo 获取kafka的配置属性
        args = new String[]{"--input-topic", "topn_test", "--bootstrap.servers", "node2.hadoop:9092,node3.hadoop:9092",
                "--zookeeper.connect", "node1.hadoop:2181,node2.hadoop:2181,node3.hadoop:2181", "--group.id", "cc2"};


        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        Properties sendPros = parameterTool.getProperties();
        Properties pros = parameterTool.getProperties();

        //todo 指定输入数据为kafka topic
        String topic = "orderflinktopic";
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","192.168.140.145:9092,192.168.140.146:9092,192.168.140.147:9092");
        prop.setProperty("group.id","flinkOrder");
        FlinkKafkaConsumer<String> stringFlinkKafkaConsumer = new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), prop);
        DataStreamSource<String> kafkaDstream = env.addSource(stringFlinkKafkaConsumer);


     //   DataStream<String> kafkaDstream = env.addSource(new FlinkKafkaConsumer010<String>( pros.getProperty("input-topic"),new SimpleStringSchema(),pros).setStartFromLatest()


        //todo 先转成JSON

        DataStream<JSONObject> str2JsonDstream = kafkaDstream.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String input) throws Exception {
                JSONObject inputJson = null;
                try {
                    inputJson = JSONObject.parseObject(input);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return inputJson;
            }
        });

        //todo 使用window算子
        str2JsonDstream.keyBy(value -> value.getString("appKey"))
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(2000)))
                .trigger(new DayTrigger());
              //  .process(new DayProcessOperator())
             //   .print();
        try {
            env.execute("开始执行....................");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}