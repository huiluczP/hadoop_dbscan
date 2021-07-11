package com.huiluczP.findcore;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

// 把满足相似度条件的点对分离，格式为（id1 id2）
// 输出key为id1字符串，value为id2字符串
public class FindCoreMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        if(tokenizer.countTokens() == 2){
            // 稍微检测一下免得格式出错
            // 点对必定只能有两个值
            String id1 = tokenizer.nextToken();
            String id2 = tokenizer.nextToken();
            context.write(new Text(id1), new Text(id2));
        }
    }
}
