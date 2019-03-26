import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;

/**
 * fastJson的用法，以及各类型的转化
 */
public class fastjosnTest {
    public static void main(String[] args) {
        ArrayList<Student> list = new ArrayList<>();
        Student xiaoH = new Student("xiaoH", 9);
        Student xiaoD = new Student("xiaoD", 11);

        list.add(xiaoH);
        list.add(xiaoD);

        System.out.println("* javaBean  2   jsonString" + "\n");

        String str1 = JSON.toJSONString(xiaoD);
        System.out.println(str1);

        String str2 = JSON.toJSONString(list);
        System.out.println(str2);

        System.out.println("** javaBean  2     jsonObject" + "\n\r");
        JSONObject jsonObject1 = (JSONObject) JSON.toJSON(xiaoD);
        String name1 = jsonObject1.getString("name");
        System.out.println(name1);

        /*System.out.println("jsonString    2    javabean");
        JSON.parseObject(xiaoD,Student.class);;
        System.out.println(str1);
        */


        System.out.println("*** jsonObject    2       javaBean" + "\r");
        Student student1 = JSON.toJavaObject(jsonObject1, Student.class);
        System.out.println(student1);

        System.out.println("****  javaBean    2    jsonArray" + "\r");
        ArrayList<Student> list1 = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            list1.add(new Student("student" + i, i));
        }
        JSONArray jsonArray = (JSONArray) JSON.toJSON(list1);
        for (int i = 0; i < list1.size(); i++) {
            System.out.println(jsonArray.get(i));
        }


        System.out.println("*****  jsonArray   2     javalist" + "\r");
        ArrayList<Student> list2 = new ArrayList<>();
        for (int i = 0; i < jsonArray.size(); i++) {
            Student student = JSON.toJavaObject(jsonArray.getJSONObject(i), Student.class);
            list2.add(student);
            for (Student stu :
                    list2) {
                System.out.println(stu);

            }
        }

        System.out.println("******   jsonObject  to jsonString " + "\t");
        String string = JSON.toJSONString(jsonObject1);
        System.out.println(string);

        System.out.println("*******      jsonString    2   jsonObject");
        JSONObject jsonObject = JSON.parseObject(string);

        System.out.println(jsonObject.getString("name"));

        System.out.println("********    jsonString   2    jsonArray");
        JSONArray jsonArray2 = JSON.parseArray(JSON.toJSONString(list1));
        for (int i = 0; i < jsonArray2.size(); i++) {
            System.out.println(jsonArray.getJSONObject(i));
        }


    }
}
