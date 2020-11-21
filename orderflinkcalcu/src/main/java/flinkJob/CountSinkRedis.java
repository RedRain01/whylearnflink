package flinkJob;

import com.alibaba.fastjson.JSON;
import model.CountOrderAgg;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.sql.Timestamp;
import java.util.Properties;

/**
 * @author ：why
 * @description：TODO
 * @date ：2020/10/27 16:50
 */


public class CountSinkRedis {
    public static void main(String[] args) throws Exception {
        //获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint配置
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        String topic = "orderflinktopic";
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","192.168.140.145:9092,192.168.140.146:9092,192.168.140.147:9092");
        prop.setProperty("group.id","flinkOrder");
        FlinkKafkaConsumer<String> stringFlinkKafkaConsumer = new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), prop);
        DataStreamSource<String> stringDataStreamSource = env.addSource(stringFlinkKafkaConsumer);

        DataStream<Tuple3<String,Double,String>>  dataStreamTuple3= stringDataStreamSource.map(new MapFunction<String, Tuple3<String,Double,String>>() {
            @Override
            public Tuple3<String,Double,String> map(String value) throws Exception {
                String[] orderSplit = value.split("\",\"");
                Tuple3<String,Double,String> tuple3 = new Tuple3<String,Double,String>();
                tuple3.f0 = orderSplit[1];
                tuple3.f1 = Double.valueOf(orderSplit[5]);
                tuple3.f2 = orderSplit[7];
                return tuple3;
            }
        });
        //keyby商品id
        SingleOutputStreamOperator<CountOrderAgg> aggregate = dataStreamTuple3.keyBy(0)
                .timeWindow(Time.seconds(1*60*5))
                .trigger(new Trigger<Tuple3<String, Double, String>, TimeWindow>() {
                    @Override
                    public TriggerResult onElement(Tuple3<String, Double, String> element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
                        return TriggerResult.FIRE;
                    }

                    @Override
                    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

                    }
                })
                .aggregate(new AggregateFunction<Tuple3<String,Double,String>, CountOrderAgg, CountOrderAgg>() {
                    @Override
                    public CountOrderAgg createAccumulator() {
                        return new CountOrderAgg();
                    }

                    @Override
                    public CountOrderAgg add(Tuple3<String,Double, String> tuple3, CountOrderAgg accumulator) {
                        if (accumulator.getCountAmount() == null) {
                            accumulator.setCount(1);
                            accumulator.setCountAmount(0.0);
                        } else {
                            accumulator.setCount(accumulator.getCount() + 1);
                        }
                        accumulator.setCountAmount(tuple3.f1+accumulator.getCountAmount());
                        accumulator.setSubject(tuple3.f0);
                        accumulator.setOrderTime(tuple3.f2);
                        return accumulator;
                    }

                    @Override
                    public CountOrderAgg getResult(CountOrderAgg accumulator) {
                        return accumulator;
                    }

                    @Override
                    public CountOrderAgg merge(CountOrderAgg a, CountOrderAgg b) {
                        return null;
                    }
                });

        //创建redis的配置
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("192.168.140.130").setPort(6379).build();
        //创建redissink
        RedisSink<CountOrderAgg> redisSink = new RedisSink<>(conf, new MyRedisMapper());
        aggregate.addSink(redisSink);
        aggregate.print().setParallelism(1);
        env.execute("countPort");
    }

    public static class MyRedisMapper implements RedisMapper<CountOrderAgg> {
        //表示从接收的数据中获取需要操作的redis key
        @Override
        public String getKeyFromData(CountOrderAgg data) {
            return data.getSubject();
        }
        //表示从接收的数据中获取需要操作的redis value
        @Override
        public String getValueFromData(CountOrderAgg data) {
            return JSON.toJSONString(data);
        }

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.SET);
        }
    }

}
