package flinkJob.mapclass;

import com.alibaba.fastjson.JSONObject;
import model.OrderDw;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * @author ：why
 * @description：TODO
 * @date ：2020/10/26 20:49
 */


public class orderRichMap extends RichMapFunction<String, RowData> {
    Map<String, OrderDw> oderMap = new HashMap<String, OrderDw>();
    @Override
    public void open(Configuration parameters) throws Exception {
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
            String sql = "select user_name,user_id,phone,addr,email,grade from user";
            ResultSet rs = stmt.executeQuery(sql);
            // 展开结果集数据库
            while(rs.next()){
                // 通过字段检索
                OrderDw orderDw=new OrderDw();
                String user_name  = rs.getString("user_name");
                String user_id  = rs.getString("user_id");
                String phone  = rs.getString("phone");
                String addr  = rs.getString("addr");
                String email = rs.getString("email");
                String grade = rs.getString("grade");
                orderDw.setUser_name(user_name);
                orderDw.setUser_id(user_id);
                orderDw.setPhone(phone);
                orderDw.setAddr(addr);
                orderDw.setEmail(email);
                orderDw.setGrade(grade);
                oderMap.put(user_id,orderDw);
            }
            // 完成后关闭
            rs.close();
            stmt.close();
            conn.close();
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
        super.open(parameters);
    }

    @Override
    public RowData map(String value) throws Exception {
        String[] split = value.split("\",\"");
        GenericRowData rowData = new GenericRowData(15);
        OrderDw orderDw = oderMap.get(split[3]);
        rowData.setField(0, StringData.fromString(split[0].substring(1,split[0].length())));
        rowData.setField(1, StringData.fromString(split[1]));
        rowData.setField(2, StringData.fromString(split[2]));
        rowData.setField(3, StringData.fromString(split[3]));
        rowData.setField(4, StringData.fromString(split[4]));
        rowData.setField(5, StringData.fromString(split[5]));
        rowData.setField(6, StringData.fromString(split[6]));
        rowData.setField(7, StringData.fromString(split[7]));
        rowData.setField(8, StringData.fromString(split[8]));
        rowData.setField(9, StringData.fromString(split[9].substring(0,split[9].length()-1)));
        rowData.setField(10, StringData.fromString(orderDw.getUser_name()));
        rowData.setField(11, StringData.fromString(orderDw.getUser_id()));
        rowData.setField(12, StringData.fromString(orderDw.getPhone()));
        rowData.setField(13, StringData.fromString(orderDw.getEmail()));
        rowData.setField(14, StringData.fromString(orderDw.getGrade()));
        return rowData;
    }
}
