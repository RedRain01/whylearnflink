package runmethod.window;

import model.Student;
import model.SubCoutScore;
import mysource.MyDemoSource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 测试window-aggregate
 * 按科目计算学生的成绩和科目出现的次数，因为没科分数都是一，所以次数与分数应该是一样的
 * @date ：2020/9/28 22:34
 */


public class Flink_window_agg {

    public static void main(String[] args) throws Exception {
        //获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Student> dataStream = env.addSource(new MyDemoSource());
        SingleOutputStreamOperator<SubCoutScore> aggregate = dataStream.keyBy("score")
                .timeWindow(Time.seconds(5))
                .aggregate(new AggregateFunction<Student, SubCoutScore, SubCoutScore>() {
                    @Override
                    public SubCoutScore createAccumulator() { //创建一个数据统计的容器，提供给后续操作使用。
                        return new SubCoutScore();
                    }

                    @Override
                    public SubCoutScore add(Student student, SubCoutScore acc) { //每个元素被添加进窗口的时候调用。
                        System.out.println();
                        if (acc.getCount() == 0) {
                            acc.setCount(1);
                        } else {
                            acc.setCount(acc.getCount() + 1);
                        }
                        acc.setSub(student.getSubject());
                        acc.setScoreCount(student.getScore()+acc.getScoreCount());
                        return acc;
                    }


                    @Override
                    public SubCoutScore getResult(SubCoutScore acc) {
                        //窗口统计事件触发时调用来返回出统计的结果。
                        return acc;
                    }

                    @Override
                    public SubCoutScore merge(SubCoutScore acc1, SubCoutScore acc2) { //只有在当窗口合并的时候调用,合并2个容器
                        System.out.println("---------"+acc1.getCount()+"---------------------------"+acc2.getCount()+"-------------------");
                        acc1.setCount(acc1.getCount() + acc2.getCount());
                        acc1.setScoreCount(acc1.getScoreCount() + acc2.getScoreCount());
                        return acc1;
                    }
                }, new WindowFunction<SubCoutScore, SubCoutScore, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<SubCoutScore> elements, Collector<SubCoutScore> out) throws Exception {
                        System.out.println("---tuple-------" + tuple);
                        System.out.println("-----window-----" + window);
                        for (SubCoutScore tuple2 : elements) {
                            tuple2 = tuple2;
                            out.collect(tuple2);
                        }
                    }
                });
        aggregate.print().setParallelism(1);
        env.execute("000000");
    }



}
