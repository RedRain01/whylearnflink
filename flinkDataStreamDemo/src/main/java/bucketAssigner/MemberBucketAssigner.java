package bucketAssigner;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

/**
 * @author ：why
 * @description：TODO
 * @date ：2020/10/10 21:54
 */


public class MemberBucketAssigner implements  BucketAssigner<RowData,String>{
    private static final long serialVersionUID = 10000L;

    @Override
    public String getBucketId(RowData info, Context context) {
        StringData stringData = info.getString(0);

        return "pp="+stringData.toString();
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
        return SimpleVersionedStringSerializer.INSTANCE;
    }
}
