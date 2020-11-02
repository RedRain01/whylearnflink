package orderCalculation;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author ：why
 * @description：TODO
 * @date ：2020/10/7 22:18
 */


public class KafkaToHive {
    public static void main(String[] args) throws Exception{
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

        stringDataStreamSource.print().setParallelism(1);
        env.execute("test");
    }

}
