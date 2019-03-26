package cn.com.snsoft.utils;

import java.util.Random;

public class getRandomId {
    public static String getRandomId(Integer number)  {
        String str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

        //System.out.println(str.length());

        Random random = new Random();

        StringBuffer sb = new StringBuffer();

        for (int i = 0; i < number; i++){
            int rd = random.nextInt(62);

            sb.append(str.charAt(rd));

        }
        return sb.toString();
    }

    public static void main(String[] args) {
        String random = getRandomId(32);
        System.out.println(random);
    }

}
