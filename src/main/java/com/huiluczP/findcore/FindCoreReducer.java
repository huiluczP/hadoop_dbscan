package com.huiluczP.findcore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

// 把value存到list中，不重复
// 判断list中数量，把满足core最小邻近点数量的核心点找出来
// 输出为key:coreId value:id1 id2 id3 ... idn
public class FindCoreReducer extends Reducer<Text, Text, Text, Text> {

    private int minNum = 0; // 最小邻居数

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 读取最小数量
        Configuration config = context.getConfiguration();
        minNum = config.getInt("findcore.minNum", 0);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        ArrayList<String> valueList = new ArrayList<String>();
        for(Text v:values){
            // 防止重复
            if(!valueList.contains(v.toString())){
                valueList.add(v.toString());
            }
        }
        int num = valueList.size();
        // 超过最小数目，为core(包括自己)
        if(num + 1 >= minNum){
            String idListStr = String.join(" ", valueList);
            context.write(new Text(key), new Text(idListStr));
        }
    }
}
