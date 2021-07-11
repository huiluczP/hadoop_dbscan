# hadoop_dbscan
DBSCAN achieved by hadoop apis</br>
算法实现分为七步：</br>
1. Similarity相似度计算部分，寻找满足阈值的点对组合。
2. FindCore寻找核心点，找到满足最小邻域要求的核心点及其邻域信息。
3. CoreList生成核心点列表。
4. OnlyCore转换领域信息，将其变为只有核心点的组合。
5. FirstCoreMerge按序排列，实现核心点组合第一步合并。
6. CoreMerge完成核心点合并。
7. FinalMerge 完成核心点和边界点合并，完成聚类。

项目说明：[基于mapreduce的DBSCAN算法实现](https://blog.csdn.net/qq_41733192/article/details/118658205)
