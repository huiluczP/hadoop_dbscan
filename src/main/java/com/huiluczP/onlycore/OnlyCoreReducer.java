package com.huiluczP.onlycore;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

// 原样输出
public class OnlyCoreReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String value = null;
        for(Text v:values){
            value = v.toString();
        }
        context.write(key, new Text(value));
    }
}
