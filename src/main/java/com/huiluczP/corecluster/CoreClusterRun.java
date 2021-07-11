package com.huiluczP.corecluster;

import com.huiluczP.HDbscan;
import com.huiluczP.onlycore.OnlyCoreMapper;
import com.huiluczP.onlycore.OnlyCoreReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

// 首先利用排序后的邻域信息进行合并
// 之后利用local merge进行处理
// 输入为only core
public class CoreClusterRun {

    public static void firstRun(String inputPath, String outputPath) {
        Configuration hadoopConfig = new Configuration();

        try {
            Job job = Job.getInstance(hadoopConfig, "CoreCluster first task");

            job.setJarByClass(HDbscan.class);
            job.setMapperClass(CoreClusterMapper.class);
            job.setReducerClass(CoreClusterReducer.class);

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
            System.out.println("Core cluster first step task");
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    // 之后调用local merge进行操作
    public static void secondRun(String inputPath, String outputPath) {
        Configuration hadoopConfig = new Configuration();

        try {
            Job job = Job.getInstance(hadoopConfig, "CoreCluster second task");

            job.setJarByClass(HDbscan.class);
            job.setMapperClass(CoreClusterMergeMapper.class);
            job.setReducerClass(CoreClusterMergeReducer.class);

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
            System.out.println("Core cluster second step task");
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
