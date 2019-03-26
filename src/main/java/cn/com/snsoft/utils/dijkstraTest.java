package cn.com.snsoft.utils;

public class dijkstraTest {
    public static void main(String[] args) {
        String[] tag1 = {"起亚", "实拍", "汽车", "新闻", "广州车展", "东风", "咨询"};
        String[] tag2 = {"广州", "现场", "汽车", "国际车展", "新闻", "首发", "资讯", "现代", "概念", "北京"};


        int length1 = tag1.length;
        int length2 = tag2.length;

        int[][] ints = new int[length1 + 1][length2 + 1];

        for (int a = 0; a < length1; a++) {
            ints[a][0] = a;

        }
        for (int a = 0; a < length2; a++) {
            ints[0][a] = a;

        }

        int temp;

        for (int i = 0; i < length1 - 1; i++) {
            for (int j = 0; j < length2 - 1; j++) {


            }
        }

        System.out.println(length1 + "    " + length2);

    }
}
