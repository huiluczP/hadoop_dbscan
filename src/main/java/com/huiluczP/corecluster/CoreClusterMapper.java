package com.huiluczP.corecluster;

import com.huiluczP.Util.CalUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

// 将coreid id1 id2 id3 ... idn从小到大排序
// reduce合并，这样可以减少一部分
// 最后使用local merge
// 输入为only_core
public class CoreClusterMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // 排序操作
        String[] result = CalUtil.sortArrByIntegerList(value.toString());
        System.out.println(result[0]);
        System.out.println(result[1]);
        context.write(new Text(result[0]), new Text(result[1]));
    }
}
