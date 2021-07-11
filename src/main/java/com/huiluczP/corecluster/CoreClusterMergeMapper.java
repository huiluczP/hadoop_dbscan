package com.huiluczP.corecluster;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

// 伪map-reduce结构，全部整到一个reducer中进行
public class CoreClusterMergeMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        context.write(new Text("?"), new Text(value));
    }
}
