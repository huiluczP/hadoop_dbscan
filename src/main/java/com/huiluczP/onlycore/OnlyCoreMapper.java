package com.huiluczP.onlycore;

import com.huiluczP.Util.DataUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

// 目的是将核心点邻域中非核心点整掉，方便后面计算
// 输入格式为 coreid id1 id2 ... idn
public class OnlyCoreMapper extends Mapper<Object, Text, Text, Text>{

    private ArrayList<String> coreList = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 先把core读进来
        Configuration config = context.getConfiguration();
        String filePath = config.get("corelist.core_list_filepath");
        coreList = DataUtil.readCoreList(filePath);
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // 直接在map中处理，reduce中组装就完事了
        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        ArrayList<String> core_neighbor_list = new ArrayList<String>();
        String coreId = tokenizer.nextToken();
        while(tokenizer.hasMoreTokens()){
            String token = tokenizer.nextToken();
            if(coreList.contains(token)){
                // 为核心点，则放入
                core_neighbor_list.add(token);
            }
        }
        if(core_neighbor_list.size() > 0){
            // 核心点包核心点
            String core_neighbor_str = String.join(" ", core_neighbor_list);
            context.write(new Text(coreId), new Text(core_neighbor_str));
        }else{
            // 单一核心点，输出个空格好了
            context.write(new Text(coreId), new Text(" "));
        }
    }
}
