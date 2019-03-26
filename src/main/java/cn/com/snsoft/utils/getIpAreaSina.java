package cn.com.snsoft.utils;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

public class getIpAreaSina {
    public static void main(String[] args) {
        String cityNameBySinaAPI = getCityNameBySinaAPI("139.211.216.181");
        System.out.println(cityNameBySinaAPI);

    }


    public static String getCityNameBySinaAPI(String ip) {
        String url = "http://int.dpool.sina.com.cn/iplookup/iplookup.php?format=js"
                + ip;
        String cityName = "";
        HttpClient client = HttpClientBuilder.create().build();
        HttpGet request = new HttpGet(url);
        try {
            HttpResponse response = client.execute(request);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == HttpStatus.SC_OK) {
                String strResult = EntityUtils.toString(response.getEntity());
                try {
                    JSONObject jsonResult = JSON.parseObject(strResult);
                    cityName = jsonResult.getString("city");
                    System.out.println(JSON.toJSONString(jsonResult, true));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return cityName;
    }
}
