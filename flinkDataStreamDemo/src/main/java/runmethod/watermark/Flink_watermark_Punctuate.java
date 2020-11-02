package runmethod.watermark;

import model.Student2;
import mysource.MyDemoSourceWatermark;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;

/**
 * watermark：水印
 * 目的：解决算子乱序问题，乱序问题的本质是数据产生时间和数据进入flink的时间顺序不一致，watermar本身也是插入数据的一个时间元素，通过设置合理的watermark
 * 达到降低延迟的作用
 * 时间：
 * 1：event time  事件时间
 * 2：ingestion time  进入flink时间
 * 3：processing time  算子时间
 *
 * @author ：why
 * @description：TODO
 * @date ：2020/9/28 22:34
 */


public class Flink_watermark_Punctuate {

    public static void main(String[] args) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        /*       2020-09-26 00:00:02 /2020-09-26 00:00:03 /2020-09-26 00:00:36/ 2020-09-26 00:00:04/ 2020-09-26 00:00:07  /2020-09-26 00:00:10....
         * 制造的延迟数据时间是，1601049602000，     1601049603000，1601049636000，1601049606000，1601049604000。。。。。
         * 输出：1601049602000 1601049603000  1601049604000 1601049605000。。。。1601049636000
         * */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //水印必须使用eventTime 事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<Student2> studentDataStreamSource = env.addSource(new MyDemoSourceWatermark());
        DataStream<Student2> student2DataStream = studentDataStreamSource.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Student2>() {
            Long currentMaxTimestamp = 0L;
            final Long maxOutOfOrderness = 3000L;// 最大允许的乱序时间是4s

//            @Nullable
//            @Override

            @Nullable
            @Override
            public Watermark checkAndGetNextWatermark(Student2 lastElement, long extractedTimestamp) {
                return    new Watermark(currentMaxTimestamp-maxOutOfOrderness);
            }
//            public Watermark getCurrentWatermark() {
//                return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
//            }

            @Override
            public long extractTimestamp(Student2 element, long recordTimestamp) {
                currentMaxTimestamp =element.getTime();
                return currentMaxTimestamp;
            }
        });
        OutputTag<Student2> outputTag = new OutputTag<Student2>("late-data"){};
        DataStream<Student2> score = student2DataStream.keyBy("score")
                .timeWindow(Time.seconds(2))
                .sideOutputLateData(outputTag)
                .reduce(new ReduceFunction<Student2>() {
                    @Override
                    public Student2 reduce(Student2 value1, Student2 value2) throws Exception {
                        value1.setSubject(value1.getSubject()+value2.getSubject());
                        value1.setScore(value1.getScore()+value2.getScore());
                        return value1;
                    }
                });
        score.print().setParallelism(1);
        DataStream<Student2> sideOutput = ((SingleOutputStreamOperator<Student2>) score).getSideOutput(outputTag);
        SingleOutputStreamOperator<Student2> map = sideOutput.map(new MapFunction<Student2, Student2>() {
            @Override
            public Student2 map(Student2 student2) throws Exception {
                student2.setTuition(8888888);
                return student2;
            }
        });
        //((SingleOutputStreamOperator<Student2>) score).getSideOutput("late-data");
        map.print().setParallelism(1);
        String jobName = Flink_watermark_Punctuate.class.getSimpleName();
        env.execute(jobName);
    }


}
