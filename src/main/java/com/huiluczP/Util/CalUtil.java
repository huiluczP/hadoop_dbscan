package com.huiluczP.Util;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.StringTokenizer;

// 计算相关工具类
public class CalUtil {

    // 把读取的整行转为想要的字符串信息
    // 返回 id 和 特征字符串
    public static String[] splitElementIntoArrStr(String line){
        String[] results = new String[2];
        StringTokenizer tokenizer = new StringTokenizer(line);
        String idStr = tokenizer.nextToken();
        StringBuilder sb = new StringBuilder();
        while (tokenizer.hasMoreTokens()){
            sb.append(tokenizer.nextToken()).append(" ");
        }
        sb.subSequence(0, sb.length() - 1 );
        results[0] = idStr;
        results[1] = sb.toString();
        return results;
    }

    // 将字符串分割为数字集合
    private static ArrayList<Double> splitStringIntoDoubleArr(String element){
        ArrayList<Double> elementArray = new ArrayList<>();
        String[] strArr = element.split(" ");
        for(String s:strArr){
            elementArray.add(Double.parseDouble(s));
        }
        return elementArray;
    }

    // 计算距离
    private static double calDistance(ArrayList<Double> element1, ArrayList<Double> element2){
        double dis = 0;
        for(int i=0;i<element1.size();i++){
            double temple = (element1.get(i) - element2.get(i)) * (element1.get(i) - element2.get(i));
            dis += temple;
        }
        return Math.sqrt(dis);
    }

    // 计算两点间距离是否小于阈值
    public static boolean isDistanceBelowThreshold(String element1, String element2, double thresold){
        ArrayList<Double> elementArray1 = splitStringIntoDoubleArr(element1);
        ArrayList<Double> elementArray2 = splitStringIntoDoubleArr(element2);
        double distance = calDistance(elementArray1, elementArray2);
        return distance < thresold;
    }

    // integerList排序单独拿出来作为方法
    public static ArrayList<Integer> sortIntegerList(ArrayList<Integer> intList){
        intList.sort(new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                if(o1 < o2)
                    return -1;
                else if(o1 > o2)
                    return 1;
                else
                    return 0;
            }
        });
        return intList;
    }

    // 将字符串arr排序后输出，从小到大
    public static String[] sortArrByIntegerList(String line){
        ArrayList<Integer> intList = convertStrIntoIntegerList(line);
        intList = sortIntegerList(intList);
        // 最小的核作为key
        String keyId = String.valueOf(intList.get(0));
        // 剩下的连起来
        StringBuilder sb = new StringBuilder();
        for(int i=1;i<intList.size();i++){
            sb.append(intList.get(i)).append(" ");
        }
        String[] result = new String[2];
        result[0] = keyId;
        System.out.println(intList.size());
        if(intList.size() <= 1){
            // 单个核心，输出个空格
            result[1] = " ";
        }else{
            result[1] = sb.toString().substring(0, sb.length()-1);
        }
        return result;
    }

    // 确认是否有交集
    public static boolean hasSameElement(ArrayList<String> list1, ArrayList<String> list2){
        for (String s : list1) {
            if (list2.contains(s))
                return true;
        }
        return false;
    }

    // 将list变为两个list的非重复并集
    public static void combineList(ArrayList<String> list1, ArrayList<String> list2){
        list1.removeAll(list2);
        list1.addAll(list2);
    }

    // 将string转为integer list
    public static ArrayList<Integer> convertStrIntoIntegerList(String line){
        StringTokenizer tokenizer = new StringTokenizer(line);
        ArrayList<Integer> list = new ArrayList<>();
        while (tokenizer.hasMoreTokens()){
            list.add(Integer.parseInt(tokenizer.nextToken()));
        }
        return list;
    }
}
