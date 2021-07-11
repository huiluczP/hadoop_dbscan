package com.huiluczP.corecluster;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;


// 进行二次merge操作
// 对第一次mr未完成的部分进行操作
public class CoreClusterMergeReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 转换为双层字符串list
        ArrayList<ArrayList<String>> list = new ArrayList<ArrayList<String>>();
        for(Text v:values){
            StringTokenizer tokenizer = new StringTokenizer(v.toString());
            ArrayList<String> singleList = new ArrayList<String>();
            while(tokenizer.hasMoreTokens()){
                singleList.add(tokenizer.nextToken());
            }
            list.add(singleList);
        }
        // merge core
        ArrayList<String> mergedCoreList = CoreClusterMergeAdaptor.onlyCoreMerge(list);
        // 将第一个值作为key，其他作为value输出
        for(String mc:mergedCoreList){
            int firstBlankIndex = mc.indexOf(" ");
            if(firstBlankIndex < 0){
                // 一个核心点
                context.write(new Text(mc), new Text(" "));
            }else{
                context.write(new Text(mc.substring(0, firstBlankIndex)), new Text(mc.substring(firstBlankIndex + 1)));
            }
        }
    }
}
