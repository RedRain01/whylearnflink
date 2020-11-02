
    public static class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {

        @Override
        public Long createAccumulator() { //创建一个数据统计的容器，提供给后续操作使用。
            return 0L;
        }

        @Override
        public Long add(UserBehavior userBehavior, Long acc) { //每个元素被添加进窗口的时候调用。
            return acc + 1;
        }

        @Override
        public Long getResult(Long acc) {
            ;//窗口统计事件触发时调用来返回出统计的结果。
            return acc;
        }

        @Override
        public Long merge(Long acc1, Long acc2) { //只有在当窗口合并的时候调用,合并2个容器
            return acc1 + acc2;
        }
    }

    // todo 指定格式输出 ：将每个 key每个窗口聚合后的结果带上其他信息进行输出  进入的数据为Long 返回 ItemViewCount对象
    public static class WindowResultFunction implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {

        @Override
        public void apply(
                Tuple key,  // 窗口的主键，即 itemId
                TimeWindow window,  // 窗口
                Iterable<Long> aggregateResult, // 聚合函数的结果，即 count 值
                Collector<ItemViewCount> collector  // 输出类型为 ItemViewCount
        ) throws Exception {
            Long itemId = ((Tuple1<Long>) key).f0;
            Long count = aggregateResult.iterator().next();
            collector.collect(ItemViewCount.of(itemId, window.getEnd(), count));
        }
    }