package cn.com.snsoft.utils;

import net.sf.json.JSONObject;
import org.apache.commons.lang.StringEscapeUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public class getIpArea {

    public static void main(String[] args) {
        Map<String, String> map = getIpArea("61.237.160.253 ");
        System.out.println(map);

    }


    public static Map<String,String> getIpArea(String ip){

        String path = "http://ip.taobao.com/service/getIpInfo.php?ip="+ip;
        String inputline="";
        String info="";

        try {
            URL url = new URL(path);
            HttpURLConnection conn = (HttpURLConnection)url.openConnection();
            conn.setReadTimeout(10*1000);
            conn.setRequestMethod("GET");

            InputStreamReader  inStream = new InputStreamReader(conn.getInputStream(),"UTF-8");
            BufferedReader buffer=new BufferedReader(inStream);

            while((inputline=buffer.readLine())!=null){
                info+=inputline;
            }

        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
             e.printStackTrace();
        }

        Map<String, String> map = new HashMap<>();

        JSONObject jsonob = JSONObject.fromObject((JSONObject.fromObject(info).getString("data")));
        //todo country 中国 region 北京 city 北京 county xxx ISP 电信
        String region = StringEscapeUtils.escapeSql(jsonob.getString("region"));
        String city = StringEscapeUtils.escapeSql(jsonob.getString("city"));
        String isp = StringEscapeUtils.escapeSql(jsonob.getString("isp"));

        map.put("region",region);
        map.put("city",city);
        map.put("isp",isp);

        return map;
    }
}
