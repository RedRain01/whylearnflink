package runmethod.sink;

import bucketAssigner.MemberBucketAssigner;
import org.apache.flink.core.fs.Path;
import org.apache.flink.orc.OrcSplitReaderUtil;
import org.apache.flink.orc.vector.RowDataVectorizer;
import org.apache.flink.orc.writer.OrcBulkWriterFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.hadoop.conf.Configuration;
import org.apache.orc.TypeDescription;
import utils.Util;

import java.util.Properties;

/**
 * @author ：why
 * @description：TODO
 * @date ：2020/10/10 23:14
 */


public class flink_sink_Hdfs {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);
        env.setParallelism(1);
        DataStream<RowData> dataStream = env.addSource(new MySource());

        //写入orc格式的属性
        final Properties writerProps = new Properties();
        writerProps.setProperty("orc.compress", "LZ4");

        //定义类型和字段名
        LogicalType[] orcTypes = new LogicalType[]{new VarCharType(), new VarCharType()};
        String[] fields = new String[]{"aa", "bb"};
        TypeDescription typeDescription = OrcSplitReaderUtil.logicalTypeToOrcType(RowType.of(orcTypes, fields));

        //构造工厂类OrcBulkWriterFactory
        final OrcBulkWriterFactory<RowData> factory = new OrcBulkWriterFactory<>(
                new RowDataVectorizer(typeDescription.toString(), orcTypes),
                writerProps,
                new Configuration());

        StreamingFileSink orcSink = StreamingFileSink
                .forBulkFormat(new Path("hdfs://192.168.140.138:9000/user/hive/warehouse/myhive.db/aaa"), factory)
                .withBucketAssigner(new MemberBucketAssigner())

               // .withBucketAssigner(new OnCheckpointRollingPolicy<Integer,Integer>())
//                .withNewBucketAssigner(new MemberBucketAssigner())
//                .createBuckets(123)
//                .onProcessingTime(12354)
                .build();
        dataStream.addSink(orcSink);

        env.execute();
    }

    public static class MySource implements SourceFunction<RowData> {
        private int num = 0;
        private boolean isRunning = true;

        @Override
        public void run(SourceContext<RowData> sourceContext) throws Exception {
            while (isRunning) {
                GenericRowData rowData = new GenericRowData(2);
                rowData.setField(0, org.apache.flink.table.data.StringData.fromString(Util.getClazz()));
                rowData.setField(1, org.apache.flink.table.data.StringData.fromString(Util.getSubjec()));
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
