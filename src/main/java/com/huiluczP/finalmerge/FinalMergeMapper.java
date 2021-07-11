package com.huiluczP.finalmerge;

import com.huiluczP.Util.CalUtil;
import com.huiluczP.Util.DataUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

// 最终步骤
// 将core_cluster的结果和非核心点进行合并
// 这个mapper对普通人进行读取，把key变为core cluster中的某一个
// setup中读取core_cluster
public class FinalMergeMapper extends Mapper<Object, Text, Text, Text> {

    private ArrayList<ArrayList<String>> core_cluster_result = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration config = context.getConfiguration();
        String filePath = config.get("merge.core_cluster_result_path");
        core_cluster_result = DataUtil.readCoreClusterResult(filePath);
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // 每行转list，查询是否有交集
        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        ArrayList<String> singleList = new ArrayList<String>();
        while(tokenizer.hasMoreTokens()){
            singleList.add(tokenizer.nextToken());
        }
        for(ArrayList<String> list:core_cluster_result){
            if(CalUtil.hasSameElement(list, singleList)){
                context.write(new Text(String.join(" ", list)), new Text(value));
                break;
            }
        }
    }
}
