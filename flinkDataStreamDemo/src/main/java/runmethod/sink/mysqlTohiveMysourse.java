package runmethod.sink;

import bucketAssigner.MemberBucketAssigner;
import org.apache.flink.core.fs.Path;
import org.apache.flink.orc.OrcSplitReaderUtil;
import org.apache.flink.orc.vector.RowDataVectorizer;
import org.apache.flink.orc.writer.OrcBulkWriterFactory;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.hadoop.conf.Configuration;
import org.apache.orc.TypeDescription;

import java.util.Properties;

/**
 * @author ：why
 * @description：TODO
 * @date ：2020/10/31 12:15
 */

public class mysqlTohiveMysourse {
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
            DataStream<RowData> dataStream = env.addSource(new MySource());
            //写入orc格式的属性
            final Properties writerProps = new Properties();
            writerProps.setProperty("orc.compress", "LZ4");

            //定义类型和字段名
            LogicalType[] orcTypes = new LogicalType[]{new VarCharType(), new VarCharType(), new VarCharType(), new VarCharType(), new VarCharType(), new VarCharType(), new VarCharType(), new VarCharType(), new VarCharType(), new VarCharType()};
            String[] fields = new String[]{"id","commodity_id","order_num","usercode","status","amount","phone_num","create_time","addr","order_flag"};
            TypeDescription typeDescription = OrcSplitReaderUtil.logicalTypeToOrcType(RowType.of(orcTypes, fields));

            //构造工厂类OrcBulkWriterFactory
            final OrcBulkWriterFactory<RowData> factory = new OrcBulkWriterFactory<>(
                    new RowDataVectorizer(typeDescription.toString(), orcTypes),
                    writerProps,
                    new Configuration());

            StreamingFileSink orcSink = StreamingFileSink
                    .forBulkFormat(new Path("hdfs://192.168.140.138:9000/user/hive/warehouse/ods.db/order_ods"), factory)
                    .withBucketAssigner(new MemberBucketAssigner())

                    // .withBucketAssigner(new OnCheckpointRollingPolicy<Integer,Integer>())
//                .withNewBucketAssigner(new MemberBucketAssigner())
//                .createBuckets(123)
//                .onProcessingTime(12354)
                    .build();
            dataStream.addSink(orcSink);
            env.execute("tteds");
        } catch (Exception e) {
            for (int i = 0; i < 50; i++) {
                System.out.println("------------------出现异常---------------------------------------------"+e.getMessage());

            }
        }
    }
    public static class MySource implements SourceFunction<RowData> {
        private int num = 0;
        private boolean isRunning = true;

        @Override
        public void run(SourceContext<RowData> sourceContext) throws Exception {
            while (isRunning) {
                GenericRowData rowData = new GenericRowData(10);
                rowData.setField(0, org.apache.flink.table.data.StringData.fromString("123"));
                rowData.setField(1, org.apache.flink.table.data.StringData.fromString("456"));
                rowData.setField(2, org.apache.flink.table.data.StringData.fromString("456"));
                rowData.setField(3, org.apache.flink.table.data.StringData.fromString("456"));
                rowData.setField(4, org.apache.flink.table.data.StringData.fromString("456"));
                rowData.setField(5, org.apache.flink.table.data.StringData.fromString("456"));
                rowData.setField(6, org.apache.flink.table.data.StringData.fromString("456"));
                rowData.setField(7, org.apache.flink.table.data.StringData.fromString("456"));
                rowData.setField(8, org.apache.flink.table.data.StringData.fromString("456"));
                rowData.setField(9, org.apache.flink.table.data.StringData.fromString("456"));
                sourceContext.collect(rowData);
                num++;
                if (num == 20) {
                    isRunning = false;
                }
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
