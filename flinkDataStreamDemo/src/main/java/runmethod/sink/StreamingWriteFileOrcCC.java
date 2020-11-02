package runmethod.sink;

import org.apache.flink.core.fs.Path;
import org.apache.flink.orc.OrcSplitReaderUtil;
import org.apache.flink.orc.vector.RowDataVectorizer;
import org.apache.flink.orc.writer.OrcBulkWriterFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.hadoop.conf.Configuration;
import org.apache.orc.TypeDescription;

import java.util.Properties;

/**
 * @author zhangjun 欢迎关注我的公众号[大数据技术与应用实战],获取更多精彩实战内容
 * <p>
 * StreamingFileSink 以orc格式写入
 */
public class StreamingWriteFileOrcCC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);
        env.setParallelism(1);
        DataStream<RowData> dataStream = env.addSource(
                new MySource());

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
                .forBulkFormat(new Path("hdfs://192.168.140.138:9000/user/hive/warehouse/myhive.db/cc"), factory)
                .build();
        //hdfs://master.hadoop:9000/user/hive/warehouse/myhive.db/test_orc
        //hdfs://master.hadoop:9000/user/hive/warehouse/myhive.db/abc
        dataStream.addSink(orcSink);

        env.execute();
    }

    public static class MySource implements SourceFunction<RowData> {
        @Override
        public void run(SourceContext<RowData> sourceContext) throws Exception {
            while (true) {
                GenericRowData rowData = new GenericRowData(2);

//				rowData.setField(0, (int) (Math.random() * 100));
//				rowData.setField(1, Math.random() * 100);
                rowData.setField(0, org.apache.flink.table.data.StringData.fromString("2020-10-10--10"));
                rowData.setField(1, org.apache.flink.table.data.StringData.fromString("99999"));
                StringData string = rowData.getString(1);
                System.out.println("--------------------------" + string);
                sourceContext.collect(rowData);
                Thread.sleep(10);
            }
        }
        @Override
        public void cancel() {

        }
    }

}
