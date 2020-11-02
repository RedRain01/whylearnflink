package runmethod.stream;

import model.Student;
import mysource.MyDemoSource;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * checkpoint:检查点，是flink引入的一个很重要的功能，用它来保证精确一次，数据容灾
 * 关于checkpoint的练习主要应用在，状态保存，备份重启精确一次
 * 下面是有关的配置
 *
 * @author ：why
 * @description：TODO
 * @date ：2020/9/28 22:34
 */


public class Flink_checkpoint {

    public static void main(String[] args) {
        //获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Student> dataStream = env.addSource(new MyDemoSource());
        // 每隔100 ms进行启动一个检查点
        env.enableCheckpointing(100);
        //指定事件类型
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //超时
        env.getCheckpointConfig().setCheckpointTimeout(200);
        //checkpoint失败就file掉任务，默认是true，如果希望checkpoint失败不可影响任务设置为false
        env.getCheckpointConfig().setFailOnCheckpointingErrors(true);

        //最大同时进行的checkpoint数量 默认1
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(20);
        //两次checkpoint 之间最小事件间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(20);
        //外部持久化checkpoint  参数是选择对checkpoint的处理类型 用于是否删除checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        //重启策略 任务file之后进行最大3次重启 每次间隔500ms
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 500));
        //10秒内重启三次  每次重启间隔是3s
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, org.apache.flink.api.common.time.Time.seconds(10), org.apache.flink.api.common.time.Time.seconds(3)));

    }

}
