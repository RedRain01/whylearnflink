package flinkjobDemo;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * @author ：why
 * @description：TODO
 * @date ：2020/10/24 11:52
 */


public class Test001 {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> dataStream = env.addSource(new MyNoParalleSource());
        dataStream.map(new WhoLoad()).print();
        env.execute("start...");

    }


    static class WhoLoad extends RichMapFunction<String, String> {

        private Logger logger = LoggerFactory.getLogger(WhoLoad.class);
        //缓存
        private Map<String, String> cache = new HashMap<String, String>();

        //每隔1分钟重新加载一次mysql数据到内存
        @Override
        public void open(Configuration parameters) throws Exception {
            load();
            super.open(parameters);


        }

        @Override
        public String map(String s) throws Exception {
            final String vin = cache.get(s);
            return "根据isa 获取到的vin ----------------------------`： " + vin;
        }

        //加载mysql数据到内存
        private void load() throws ClassNotFoundException, SQLException {
            // MySQL 8.0 以下版本 - JDBC 驱动名及数据库 URL
             final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
             final String DB_URL = "jdbc:mysql://192.168.140.146:3306/whyt?characterEncoding=UTF-8";

            // 数据库的用户名与密码，需要根据自己的设置
             final String USER = "root";
             final String PASS = "WHYwhy@@@123";
                Connection conn = null;
                Statement stmt = null;
                try{
                    // 注册 JDBC 驱动
                    Class.forName(JDBC_DRIVER);
                    // 打开链接
                    conn = DriverManager.getConnection(DB_URL,USER,PASS);
                    // 执行查询
                    stmt = conn.createStatement();
                    String sql = "select commodity_name,commodity_no from commodity";
                    ResultSet rs = stmt.executeQuery(sql);
                    // 展开结果集数据库
                    while(rs.next()){
                        // 通过字段检索
                        String id  = rs.getString("commodity_name");
                        String name = rs.getString("commodity_no");
                        System.out.println("--------------------------------------"+id+name);
                        cache.put(id, name);
                    }
                    // 完成后关闭
                    rs.close();
                    stmt.close();
                    conn.close();
                }catch(SQLException se){
                    // 处理 JDBC 错误
                    se.printStackTrace();
                }catch(Exception e){
                    // 处理 Class.forName 错误
                    e.printStackTrace();
                }finally{
                    // 关闭资源
                    try{
                        if(stmt!=null) stmt.close();
                    }catch(SQLException se2){

                    }// 什么都不做
                    try{
                        if(conn!=null) conn.close();
                    }catch(SQLException se){
                        se.printStackTrace();
                    }
                }

        }

    }
}
