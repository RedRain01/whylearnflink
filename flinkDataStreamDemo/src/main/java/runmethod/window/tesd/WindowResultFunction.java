package runmethod.window.tesd;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author ：why
 * @description：TODO
 * @date ：2020/10/30 12:39
 */



// todo 指定格式输出
public  class WindowResultFunction implements WindowFunction<Long, JSONObject, String, TimeWindow> {


    @Override
    public void apply(
            String key,  // 窗口的主键，即 itemId
            TimeWindow window,  // 窗口
            Iterable<Long> aggregateResult, // 聚合函数的结果，即 count 值
            Collector<JSONObject> collector  // 输出类型为 ItemViewCount
    ) throws Exception {


        Long count = aggregateResult.iterator().next();
        //窗口结束时间
        long end = window.getEnd();
        JSONObject json = new JSONObject();
        json.put("key",key);
        json.put("count",count);
        json.put("end",end);
        collector.collect(json);
    }
}