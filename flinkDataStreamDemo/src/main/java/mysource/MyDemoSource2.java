package mysource;

import model.Student;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import utils.Util;

/**
 * 使用Student对象作为数据传输对象，
 * @author ：why
 * @description：TODO
 * @date ：2020/9/16 17:29
 */


public class MyDemoSource2 implements SourceFunction<Student> {

    int num=0;
private boolean isRunning=true;
    @Override
    public void run(SourceContext<Student> sourceContext) throws Exception {
        while (isRunning){
            num++;
            Student student=new Student();
            student.setName(Util.getXname());
            student.setId(Util.getId());
            student.setClazz(Util.getClazz());
            student.setSubject(Util.getSubjec());
            student.setScore(Util.getScore());
            sourceContext.collect(student);
          //  System.out.println("<<------------------"+ JSON.toJSONString(student));


          /*  Student student=new Student();
            student.setName("1");
            student.setId("1");
            student.setClazz("1");
            student.setSubject("1");
            student.setScore(55);
            student.setTuition(0);
            sourceContext.collect(student);*/
            Thread.sleep(200);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
