package com.huiluczP.corelist;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

// 强行用MR来做
// 把core_neighbor最前面的core id核心点编号输出
// reduce中把编号输出成list文件
public class CoreListMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // 每一行只取最前面的一个token
        // 因为不会重复，直接key value相同输出
        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        String id = tokenizer.nextToken();
        context.write(new Text(id), new Text(id));
    }
}
