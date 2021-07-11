package com.huiluczP;

import com.huiluczP.Util.DataUtil;
import com.huiluczP.corecluster.CoreClusterRun;
import com.huiluczP.corelist.CoreListRun;
import com.huiluczP.finalmerge.FinalMergeRun;
import com.huiluczP.findcore.FindCoreRun;
import com.huiluczP.onlycore.OnlyCoreRun;
import com.huiluczP.similarity.SimilarityRun;

// 主实现类，到时候job全塞到里面
public class HDbscan {
    public static void main(String args[]){
        // 各项设置
        String dataPath = DataUtil.HDFS_INPUT + "/" + args[2]; // 数据集地址
        String similarityPairPath = DataUtil.HDFS_OUTPUT + "/pair.txt"; // 满足阈值的点对结果输出地址
        String coreNeighborPath = DataUtil.HDFS_OUTPUT + "/core_neighbor.txt"; // 核心点及其邻域点输出地址
        String coreListPath = DataUtil.HDFS_OUTPUT + "/core_list.txt"; // 核心点列表输出地址
        String onlyCoreNeighborPath = DataUtil.HDFS_OUTPUT + "/only_core.txt"; // 仅核心点邻域输出地址
        String coreClusterFirstStep = DataUtil.HDFS_OUTPUT + "/core_cluster_first.txt"; // core cluster按序合并第一步输出地址
        String coreClusterResult = DataUtil.HDFS_OUTPUT + "/core_cluster_result.txt"; // core cluster合并最终输出地址
        String finalMergePath = DataUtil.HDFS_OUTPUT + "/final_merge.txt"; // 最终合并输出地址

        double threshold = Double.parseDouble(args[0]); // 阈值
        int minNum = Integer.parseInt(args[1]); // 成为核心的最低要求

        // SimilarityRun.run(dataPath, similarityPairPath, threshold); // 计算相似度
        FindCoreRun.run(similarityPairPath, coreNeighborPath, minNum); // 寻找核心点
        CoreListRun.run(coreNeighborPath, coreListPath); // 核心点列表
        OnlyCoreRun.run(coreNeighborPath, onlyCoreNeighborPath, coreListPath); // 转为只有核心点的序列
        CoreClusterRun.firstRun(onlyCoreNeighborPath, coreClusterFirstStep); // core cluster按序合并第一步
        CoreClusterRun.secondRun(coreClusterFirstStep, coreClusterResult); // core cluster按序合并
        FinalMergeRun.run(coreNeighborPath, finalMergePath, coreClusterResult); // 最终组合，完成聚类
    }
}
