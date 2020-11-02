package mysource;

import com.alibaba.fastjson.JSON;
import model.Student;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import utils.Util;

/**
 * 一般数据源：单线程
 * @author ：why
 * @description：TODO
 * @date ：2020/9/16 17:29
 */


public class MyDemoSource implements SourceFunction<Student> {
    private int num=0;
private boolean isRunning=true;
    @Override
    public void run(SourceFunction.SourceContext<Student> sourceContext) throws Exception {
        while (isRunning){
            num++;
            Student student=new Student();
            student.setName(Util.getName());
            student.setId(Util.getId());
            student.setClazz(Util.getClazz());
            student.setSubject(Util.getSubjec());
            student.setScore(2);
            sourceContext.collect(student);
            if (num == 200) {
                isRunning = false;
            }
            Thread.sleep(200);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
