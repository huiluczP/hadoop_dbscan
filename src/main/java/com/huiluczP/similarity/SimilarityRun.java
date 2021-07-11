package com.huiluczP.similarity;

import com.huiluczP.HDbscan;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SimilarityRun {
    // 相似度计算job设置
    // inputPath为完整的data文件地址，output为满足阈值条件的pair输出地址
    // threshold为阈值大小
    public static void run(String inputPath, String outputPath, double threshold){
        Configuration hadoopConfig = new Configuration();

        // 向工作配置中设置文件地址和阈值信息
        hadoopConfig.set("data.filepath", inputPath);
        hadoopConfig.setDouble("similarity.threshold", threshold);

        try {
                Job job = Job.getInstance(hadoopConfig, "similarity task");

                job.setJarByClass(HDbscan.class);
                job.setMapperClass(SimilarityMapper.class);
                job.setReducerClass(SimilarityReducer.class);

                job.setMapOutputKeyClass(IntWritable.class);
                job.setMapOutputValueClass(Text.class);

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);

                // 输出已存在则删除
                Path outPath = new Path(outputPath);
                outPath.getFileSystem(hadoopConfig).delete(outPath, true);
                //job执行作业时输入和输出文件的路径
                FileInputFormat.addInputPath(job, new Path(inputPath));
                FileOutputFormat.setOutputPath(job, new Path(outputPath));

                //执行job，直到完成
                job.waitForCompletion(true);
                System.out.println("Finished Similarity task");
            } catch (IOException | InterruptedException | ClassNotFoundException e) {
                e.printStackTrace();
            }
    }
}
