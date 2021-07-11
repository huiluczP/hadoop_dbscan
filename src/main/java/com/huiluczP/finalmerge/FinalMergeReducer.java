package com.huiluczP.finalmerge;

import com.huiluczP.Util.CalUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

// 将相同key的组合合并
public class FinalMergeReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 将value全部整合在同一个list中
        ArrayList<String> list = new ArrayList<String>();
        for(Text v:values){
            StringTokenizer tokenizer = new StringTokenizer(v.toString());
            ArrayList<String> singleList = new ArrayList<String>();
            while(tokenizer.hasMoreTokens()){
                singleList.add(tokenizer.nextToken());
            }
            // 获取非重复并集
            CalUtil.combineList(list, singleList);
        }
        // 输出结果
        String clusterStr = String.join(" ", list);
        int firstBlankIndex = clusterStr.indexOf(" ");
        context.write(new Text(clusterStr.substring(0, firstBlankIndex)), new Text(clusterStr.substring(firstBlankIndex + 1)));
    }
}
