package com.huiluczP.corecluster;

import com.huiluczP.Util.CalUtil;
import com.huiluczP.Util.DataUtil;

import java.io.IOException;
import java.util.ArrayList;

public class CoreClusterMergeAdaptor {

    // core local merge过程
    public static ArrayList<String> onlyCoreMerge(ArrayList<ArrayList<String>> coreList){
        // 0为未被使用，1为已被使用并作为簇保留，2为被吸干
        int[] isUsed = new int[coreList.size()];
        for(int i=0;i<coreList.size();i++){
            ArrayList<String> now = coreList.get(i);
            if(isUsed[i] == 0){
                // 找到包含now中某个值的list，并全部吸干
                for(int j=0;j<coreList.size();j++){
                    // 可吸选手
                    if(isUsed[j]!=1 && isUsed[j]!=2 && i!=j){
                        // 有交集
                        if(CalUtil.hasSameElement(now, coreList.get(j))){
                            CalUtil.combineList(now, coreList.get(j));
                            isUsed[j] = 2;
                            isUsed[i] = 1;
                        }
                    }
                }
            }
        }
        ArrayList<String> result = new ArrayList<String>();
        for(int i=0;i<isUsed.length;i++){
            if(isUsed[i] == 1 || isUsed[i] == 0){
                result.add(String.join(" ", coreList.get(i)));
            }
        }
        return result;
    }

    public static void runMerge(String filePath){
        try {
            // 读取core_cluster_first文件
            ArrayList<ArrayList<String>> coreClusterFirst = DataUtil.readCoreClusterResult(filePath);
            // 执行merge
            ArrayList<String> coreResult = onlyCoreMerge(coreClusterFirst);
            // 保存结果

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
