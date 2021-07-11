package com.huiluczP.finalmerge;

import com.huiluczP.HDbscan;
import com.huiluczP.corelist.CoreListMapper;
import com.huiluczP.corelist.CoreListReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FinalMergeRun {

    // 最终合并
    public static void run(String inputPath, String outputPath, String coreClusterResultPath){
        Configuration hadoopConfig = new Configuration();
        hadoopConfig.set("merge.core_cluster_result_path", coreClusterResultPath);

        try {
            Job job = Job.getInstance(hadoopConfig, "finalMerge task");

            job.setJarByClass(HDbscan.class);
            job.setMapperClass(FinalMergeMapper.class);
            job.setReducerClass(FinalMergeReducer.class);

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
            System.out.println("Finished final merge task");
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
