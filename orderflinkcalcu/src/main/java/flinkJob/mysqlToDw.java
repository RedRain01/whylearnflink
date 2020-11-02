package flinkJob;

/**
 * @author ：why
 * @description：TODO
 * @date ：2020/10/26 20:25
 */

import bucketAssigner.MemberBucketAssigner;
import flinkJob.mapclass.orderRichMap;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.orc.OrcSplitReaderUtil;
import org.apache.flink.orc.vector.RowDataVectorizer;
import org.apache.flink.orc.writer.OrcBulkWriterFactory;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.hadoop.conf.Configuration;
import org.apache.orc.TypeDescription;

import java.util.Properties;

/**
 * 在flinkToHive的基础上，使用richMap在open方法中获取数据库连接查询维表信息放入内存
 */
public class mysqlToDw {
    public static void main(String[] args) throws Exception {
        try {
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
            SingleOutputStreamOperator<RowData> map = stringDataStreamSource.map(new orderRichMap());

            //写入orc格式的属性
            final Properties writerProps = new Properties();
            writerProps.setProperty("orc.compress", "LZ4");
            //定义类型和字段名
            LogicalType[] orcTypes = new LogicalType[]{new VarCharType(64), new VarCharType(64), new VarCharType(64), new VarCharType(64), new VarCharType(64), new VarCharType(64), new VarCharType(64), new VarCharType(64), new VarCharType(64), new VarCharType(64), new VarCharType(64), new VarCharType(64), new VarCharType(64), new VarCharType(64), new VarCharType(64)};
            String[] fields = new String[]{"id","commodity_id","order_num","usercode","status","amount","phone_num","create_time","addr","order_flag","user_name","user_id","phone","email","grade"};
            TypeDescription typeDescription = OrcSplitReaderUtil.logicalTypeToOrcType(RowType.of(orcTypes, fields));
            //构造工厂类OrcBulkWriterFactory
            final OrcBulkWriterFactory<RowData> factory = new OrcBulkWriterFactory<>(
                    new RowDataVectorizer(typeDescription.toString(), orcTypes),
                    writerProps,
                    new Configuration());
            StreamingFileSink orcSink = StreamingFileSink
                    .forBulkFormat(new Path("hdfs://192.168.140.138:9000/user/hive/warehouse/dw.db/order_dw"), factory)
                    .withBucketAssigner(new MemberBucketAssigner())
                    .build();
            map.addSink(orcSink);
            env.execute("tes92t");
        } catch (Exception e) {
            System.out.println("<<-------出现异常:----"+e.getMessage());
        }
    }
}
