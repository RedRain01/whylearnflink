//package runMethodScala
//
//import model.Student
//import mysource.MyDemoSource
//import org.apache.flink.api.common.functions.FlatMapFunction
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
//import org.apache.flink.util.Collector
//
///**此方法存放大多数基础算子 scala写法
//  * DataStream<student>-->datastream<String>
//  */
//object flinkMapScala {
//  def main(args: Array[String]): Unit = {
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//
//    val dataStream = env.addSource(new MyDemoSource)
//
//    val value = dataStream.flatMap(new FlatMapFunction[Student, String] {
//
//
//      /**
//        *dataStream<student> -->datastream<string>
//        * @param value
//        * @param out
//        */
//          val value= dataStream.map(map => {
//              map.getClazz;
//          })
//
//      /**
//        *dataStream<student> -->datastream<string> flatmap中没有迭代方法，所以没有拍扁效果，是一对一流输出
//        * @param value
//        * @param out
//        */
//
//      private val value1: SingleOutputStreamOperator[String] = dataStream.flatMap(new FlatMapFunction[Student, String] {
//        override def flatMap(value: Student, out: Collector[String]): Unit = out.collect(value.getClazz)
//      })
//
//
//      private val value2: SingleOutputStreamOperator[String] = dataStream.flatMap(new FlatMapFunction[Student, String] {
//        override def flatMap(value: Student, out: Collector[String]): Unit = {
//          out.collect(value.getClazz)
//        }
//      })
//
//
//    /**
//      *
//      * @param value
//      * @param out
//      */
//    /**
//      *
//      * @param value
//      * @param out
//      */
//
//    /**
//      *
//      * @param value
//      * @param out
//      */
//
//    /**
//      *
//      * @param value
//      * @param out
//      */
//
//    /**
//      *
//      * @param value
//      * @param out
//      */
//
//    /**
//      *
//      * @param value
//      * @param out
//      */
//
//    /**
//      *
//      * @param value
//      * @param out
//      */
//
//    /**
//      *
//      * @param value
//      * @param out
//      */
//
//    /**
//      *
//      * @param value
//      * @param out
//      */
//
//
//    value.print().setParallelism(1)
//    env.execute("test001")
//  }
//}
