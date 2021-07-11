package com.huiluczP.corelist;

import com.huiluczP.HDbscan;
import com.huiluczP.findcore.FindCoreMapper;
import com.huiluczP.findcore.FindCoreReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CoreListRun {
    // 输出core list
    public static void run(String inputPath, String outputPath){
        Configuration hadoopConfig = new Configuration();

        try {
            Job job = Job.getInstance(hadoopConfig, "coreList task");

            job.setJarByClass(HDbscan.class);
            job.setMapperClass(CoreListMapper.class);
            job.setReducerClass(CoreListReducer.class);

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
            System.out.println("Finished coreList task");
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
