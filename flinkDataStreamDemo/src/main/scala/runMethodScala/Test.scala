//package runMethodScala
//
//class JoinTest {
//  val env = StreamExecutionEnvironment.getExecutionEnvironment
//
//  //便于测试，并行度设置为1
//  env.setParallelism(1)
//
//  //env.getConfig.setAutoWatermarkInterval(9000)
//
//  //设置为事件时间
//  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//
//  //设置source 本地socket
//  val text: DataStream[String] = env.socketTextStream("localhost", 9000)
//
//
//  val lateText = new OutputTag[(String, String, Long, Long)]("late_data")
//
//  val value = text.filter(new MyFilterNullOrWhitespace)
//    .flatMap(new MyFlatMap)
//    .assignTimestampsAndWatermarks(new MyWaterMark)
//    .map(x => (x.name, x.datetime, x.timestamp, 1L))
//    .keyBy(_._1)
//    .window(TumblingEventTimeWindows.of(Time.seconds(5)))
//    .sideOutputLateData(lateText)
//    //.sum(2)
//    .apply(new MyWindow)
//  //.window(TumblingEventTimeWindows.of(Time.seconds(3)))
//  //.apply(new MyWindow)
//  value.getSideOutput(lateText).map(x => {
//    "延迟数据|name:" + x._1 + "|datetime:" + x._2
//  }).print()
//
//  value.print()
//
//  env.execute("watermark test")
//
//
//}
