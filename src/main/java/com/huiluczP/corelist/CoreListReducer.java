package com.huiluczP.corelist;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CoreListReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 直接输出，key为空
        String value = null;
        for(Text t:values){
            value = t.toString();
        }
        context.write(new Text(""), new Text(value));
    }
}
