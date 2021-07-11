package com.huiluczP.corecluster;

import com.huiluczP.Util.CalUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

// 将相同key的整合在一起
// id不重复地输出
// value集合为id1-id2..，把这些从大到小排列好了
public class CoreClusterReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String minCoreId = key.toString();
        ArrayList<Integer> uniqueId = new ArrayList<Integer>();
        // 把value作为list合并
        for(Text v:values){
            // 注意可能有单个空格值
            if(!v.toString().equals(" ")){
                ArrayList<Integer> neighbor_list = CalUtil.convertStrIntoIntegerList(v.toString());
                for(Integer intNeighbor:neighbor_list){
                    if(!uniqueId.contains(intNeighbor)){
                        uniqueId.add(intNeighbor);
                    }
                }
            }
        }
        // 排序输出
        uniqueId = CalUtil.sortIntegerList(uniqueId);
        StringBuilder sb = new StringBuilder();
        for(int i=1;i<uniqueId.size();i++){
            sb.append(uniqueId.get(i)).append(" ");
        }

        context.write(key, new Text(sb.toString()));
    }
}
