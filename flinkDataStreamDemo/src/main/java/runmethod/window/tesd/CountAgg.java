package runmethod.window.tesd;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @author ：why
 * @description：TODO
 * @date ：2020/10/30 12:39
 */


public  class CountAgg implements AggregateFunction<JSONObject, Long, Long> {


    @Override
    public Long createAccumulator() { //创建一个数据统计的容器，提供给后续操作使用。
        return 0L;
    }


    @Override
    public Long add(JSONObject json, Long acc) { //每个元素被添加进窗口的时候调用。
        return acc + 1;
    }


    @Override
    public Long getResult(Long acc) {
        //窗口统计事件触发时调用来返回出统计的结果。
        return acc;
    }


    @Override
    public Long merge(Long acc1, Long acc2) { //只有在当窗口合并的时候调用,合并2个容器
        return acc1 + acc2;
    }
}