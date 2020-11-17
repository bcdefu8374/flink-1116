package com.my.day02;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author chen
 * @topic  从文件中读取数据
 * @create 2020-11-17
 */


public class SourceTextFile_Collection {
    public static void main(String[] args) throws Exception {

        //1.创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.设置并行度
        env.setParallelism(1);

        //3.从文件读取数据
        DataStreamSource<String> readTextDS = env.readTextFile("input/sensor.txt");

        //4.打印数据
        readTextDS.print();

        //5.开启流
        env.execute("SourceTextFile_Collection");


    }
}
