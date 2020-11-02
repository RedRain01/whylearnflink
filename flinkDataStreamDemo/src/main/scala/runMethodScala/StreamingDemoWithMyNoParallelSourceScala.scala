//package runMethodScala
//
//import org.apache.flink.streaming.api.windowing.time.Time
//
///**
//  * Created by xuwei.tech on 2018/10/23.
//  */
//object StreamingDemoWithMyNoParallelSourceScala {
//
//  def main(args: Array[String]): Unit = {
//
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//
//    //隐式转换
//
//    val text = env.addSource(new MyNoParallelSourceScala)
//
//    val mapData = text.map(line=>{
//      println("接收到的数据："+line)
//      line
//    })
//
//    val sum = mapData.timeWindowAll(Time.seconds(2)).sum(0)
//
//
//    sum.print().setParallelism(1)
//
//    env.execute("StreamingDemoWithMyNoParallelSourceScala")
//
//
//
//  }
//
//}
