//package otherDemo;
//
//import org.apache.flink.api.common.functions.RichMapFunction;
//
///**
// * @author ：why
// * @description：TODO
// * @date ：2020/10/30 10:00
// */
//
//
//public static final class MapWithSiteInfoFunc
//        extends RichMapFunction<String, String> {
//    private static final Logger LOGGER = LoggerFactory.getLogger(MapWithSiteInfoFunc.class);
//    private static final long serialVersionUID = 1L;
//
//    private transient ScheduledExecutorService dbScheduler;
//    private Map<Integer, SiteAndCityInfo> siteInfoCache;
//
//    @Override
//    public void open(Configuration parameters) throws Exception {
//        super.open(parameters);
//        siteInfoCache = new HashMap<>(1024);
//
//        dbScheduler = new ScheduledThreadPoolExecutor(1, r -> {
//            Thread thread = new Thread(r, "site-info-update-thread");
//            thread.setUncaughtExceptionHandler((t, e) -> {
//                LOGGER.error("Thread " + t + " got uncaught exception: " + e);
//            });
//            return thread;
//        });
//
//        dbScheduler.scheduleWithFixedDelay(() -> {
//            try {
//                QueryRunner queryRunner = new QueryRunner(JdbcUtil.getDataSource());
//                List<Map<String, Object>> info = queryRunner.query(SITE_INFO_QUERY_SQL, new MapListHandler());
//
//                for (Map<String, Object> item : info) {
//                    siteInfoCache.put((int) item.get("site_id"), new SiteAndCityInfo(
//                            (int) item.get("site_id"),
//                            (String) item.getOrDefault("site_name", ""),
//                            (long) item.get("city_id"),
//                            (String) item.getOrDefault("city_name", "")
//                    ));
//                }
//
//                LOGGER.info("Fetched {} site info records, {} records in cache", info.size(), siteInfoCache.size());
//            } catch (Exception e) {
//                LOGGER.error("Exception occurred when querying: " + e);
//            }
//        }, 0, 10 * 60, TimeUnit.SECONDS);
//    }
//
//    @Override
//    public String map(String value) throws Exception {
//        JSONObject json = JSON.parseObject(value);
//        int siteId = json.getInteger("site_id");
//
//        String siteName = "", cityName = "";
//        SiteAndCityInfo info = siteInfoCache.getOrDefault(siteId, null);
//        if (info != null) {
//            siteName = info.getSiteName();
//            cityName = info.getCityName();
//        }
//
//        json.put("site_name", siteName);
//        json.put("city_name", cityName);
//        return json.toJSONString();
//    }
//
//    @Override
//    public void close() throws Exception {
//        siteInfoCache.clear();
//        ExecutorUtils.gracefulShutdown(10, TimeUnit.SECONDS, dbScheduler);
//        JdbcUtil.close();
//
//        super.close();
//    }
//
//    private static final String SITE_INFO_QUERY_SQL = "...";
//}