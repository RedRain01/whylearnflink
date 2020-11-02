package utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @author ：why
 * @description：TODO
 * @date ：2020/10/7 9:55
 */


public class Tes {
    public static void main(String[] args)throws Exception {
       // System.out.println(dateToStamp("2019-03-26 16:25:24"));
//         String s="\"1256\",\"c3\",\"1\",\"z2\",\"00\",\"123.0\",\"15580275047\",\"2020-10-26 15:20:33.0\",\"2\",\"8784\"";
//        String[] split = s.split("\",\"");
        JSONObject jsonObject=new JSONObject();
        JSONObject jsonObject1=new JSONObject();
        HashMap<String,String> map=new HashMap<>();
        jsonObject.put("ss","sss");
        jsonObject1.put("s22s","sss22");


//        for (int i = 0; i <100 ; i++) {
//            System.out.println(Util.getClazz());
//        }

    }
    /*
     * 将时间转换为时间戳
     */
    public static String dateToStamp(String s) throws Exception{
        String res;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = simpleDateFormat.parse(s);
        long ts = date.getTime();
        res = String.valueOf(ts);
        return res;
    }
}
