package com.huiluczP.similarity;

import com.huiluczP.Util.CalUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
相似度计算mapper
对坐标点来说就是计算距离
输入文件格式为id a0 a1 a2... an
输出（id,arr）
 */
public class SimilarityMapper extends Mapper<Object, Text, IntWritable, Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        String[] results = CalUtil.splitElementIntoArrStr(line);
        // id为key，坐标为value
        context.write(new IntWritable(Integer.parseInt(results[0])), new Text(results[1]));
    }
}
