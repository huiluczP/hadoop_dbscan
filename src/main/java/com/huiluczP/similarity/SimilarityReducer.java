package com.huiluczP.similarity;

import com.huiluczP.Util.CalUtil;
import com.huiluczP.Util.DataUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

/*
该reducer主要是把距离在阈值以下的组合输出
因为不能直接进行矩阵距离计算，每次计算一个坐标和其他坐标的距离
并按照阈值进行取舍
输出为两个字符串形式的id
 */
public class SimilarityReducer extends Reducer<IntWritable, Text, Text, Text> {

    private ArrayList<String> elementArrList = null;
    private double threshold = 0.0;

    @Override
    protected void setup(Context context) throws IOException{
        // reduce过程开始前调用一次，读取一份完整的坐标信息
        // 同时读取data信息和
        Configuration conf = context.getConfiguration();
        String filePath = conf.get("data.filepath");
        threshold = conf.getDouble("similarity.threshold", 0.0);
        elementArrList = DataUtil.readElementArrList(filePath);
    }

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 计算相似度信息
        int idCal = key.get();
        String arr = values.iterator().next().toString();
        for(int i=0;i<elementArrList.size();i++){
            if(i == idCal)
                continue;
            boolean isPaired = CalUtil.isDistanceBelowThreshold(arr, elementArrList.get(i), threshold);
            if(isPaired){
                context.write(new Text(String.valueOf(idCal)), new Text(String.valueOf(i)));
            }
        }
    }
}
