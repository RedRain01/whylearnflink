package mysource;

import com.alibaba.fastjson.JSON;
import model.Student2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import utils.Util;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * watermark的数据源
 * @author ：why
 * @description：TODO
 * @date ：2020/9/16 17:29
 */


public class MyDemoSourceWatermarkPunctuate implements SourceFunction<Student2> {

    int num=0;
private boolean isRunning=true;
    @Override
    public void run(SourceContext<Student2> sourceContext) throws Exception {
        while (isRunning){
            num++;
            Student2 student=new Student2();
            student.setName(Util.getXname());
            student.setId(Util.getId());
            student.setClazz(Util.getClazz());
            student.setSubject(Util.getSubjec()+num);
            student.setScore(1);
            student.setTime(Util.getTimePun());
            student.setTimeDate(stampToDate(student.getTime()));
            sourceContext.collect(student);
            System.out.println("<<------------------"+ JSON.toJSONString(student));
//
//            if (num == 190) {
//                isRunning = false;
//            }
          /*  Student student=new Student();
            student.setName("1");
            student.setId("1");
            student.setClazz("1");
            student.setSubject("1");
            student.setScore(55);
            student.setTuition(0);
            sourceContext.collect(student);*/
       // Thread.sleep(100);
        }
    }
    /*
     * 将时间戳转换为时间
     */
    public static String stampToDate(long time){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String time_Date = sdf.format(new Date(time));
        return time_Date;

    }
    @Override
    public void cancel() {
        isRunning = false;
    }
}
