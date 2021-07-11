package com.huiluczP.findcore;

import com.huiluczP.HDbscan;
import com.huiluczP.similarity.SimilarityMapper;
import com.huiluczP.similarity.SimilarityReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FindCoreRun {
    // 相似度计算job设置
    // inputPath为完整的data文件地址，output为满足阈值条件的pair输出地址
    // minNum为最小邻居个数
    public static void run(String inputPath, String outputPath, int minNum){
        Configuration hadoopConfig = new Configuration();

        // 向工作配置中设置minNum
        hadoopConfig.setInt("findcore.minNum", minNum);

        try {
            Job job = Job.getInstance(hadoopConfig, "findCore task");

            job.setJarByClass(HDbscan.class);
            job.setMapperClass(FindCoreMapper.class);
            job.setReducerClass(FindCoreReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            // 输出已存在则删除
            Path outPath = new Path(outputPath);
            outPath.getFileSystem(hadoopConfig).delete(outPath, true);

            //job执行作业时输入和输出文件的路径
            FileInputFormat.addInputPath(job, new Path(inputPath));
            FileOutputFormat.setOutputPath(job, new Path(outputPath));

            //执行job，直到完成
            job.waitForCompletion(true);
            System.out.println("Finished findCore task");
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
