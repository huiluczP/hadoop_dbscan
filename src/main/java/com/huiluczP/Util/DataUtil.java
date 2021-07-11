package com.huiluczP.Util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

// 文件读取相关工具类
public class DataUtil {
    public static final String HDFS_INPUT = "hdfs://huiluczPc:8020/input"; // input地址
    public static final String HDFS_OUTPUT = "hdfs://huiluczPc:8020/output"; // output地址

    // 获取文件对应的hdfs系统下的linereader
    private static LineReader getLineReader(String filePath) throws IOException {
        Path path = new Path(filePath);
        Configuration conf = new Configuration();
        FileSystem fileSystem = path.getFileSystem(conf);

        FSDataInputStream fsdis = fileSystem.open(path);
        LineReader lineReader = new LineReader(fsdis, conf);

        return lineReader;
    }

    // 读取坐标点数据，转为字符串
    public static ArrayList<String> readElementArrList(String filePath) throws IOException {
        ArrayList<String> elementArrList = new ArrayList<>();

        LineReader lineReader = getLineReader(filePath);

        Text line = new Text();
        while(lineReader.readLine(line) > 0){
            String[] result = CalUtil.splitElementIntoArrStr(line.toString());
            elementArrList.add(result[1]);
        }
        lineReader.close();
        return elementArrList;
    }

    // 读取核心点文件，转为list
    // 核心点文件为hdfs文件夹，遍历读取
    public static ArrayList<String> readCoreList(String filePath) throws IOException {
        ArrayList<String> coreList = new ArrayList<String>();

        Path path = new Path(filePath);
        Configuration conf = new Configuration();
        FileSystem fileSystem = path.getFileSystem(conf);

        // 遍历读取
        FileStatus[] listFile = fileSystem.listStatus(path);
        for (int i = 0; i < listFile.length; i++) {
            LineReader lineReader = getLineReader(listFile[i].getPath().toString());
            Text line = new Text();
            while(lineReader.readLine(line) > 0){
                StringTokenizer tokenizer = new StringTokenizer(line.toString());
                String coreId = tokenizer.nextToken();
                coreList.add(coreId);
            }
            lineReader.close();
        }
        return coreList;
    }

    // 读取core_cluster文件，转换为字符串list
    public static ArrayList<ArrayList<String>> readCoreClusterResult(String filePath) throws IOException {
        ArrayList<ArrayList<String>> results = new ArrayList<ArrayList<String>>();

        Path path = new Path(filePath);
        Configuration conf = new Configuration();
        FileSystem fileSystem = path.getFileSystem(conf);

        // 遍历读取
        FileStatus[] listFile = fileSystem.listStatus(path);
        for (int i = 0; i < listFile.length; i++) {
            LineReader lineReader = getLineReader(listFile[i].getPath().toString());
            Text line = new Text();
            // 每一行进行一次读取
            while(lineReader.readLine(line) > 0){
                ArrayList<String> lineList = new ArrayList<String>();
                StringTokenizer tokenizer = new StringTokenizer(line.toString());
                while(tokenizer.hasMoreTokens()){
                    String coreId = tokenizer.nextToken();
                    lineList.add(coreId);
                }
                results.add(lineList);
            }
            lineReader.close();
        }

        return results;
    }
}
