package com.my.day01;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author chen
 * @topic  有界流处理wordcount
 * @create 2020-11-16
 */
public class Flink02_WordCount_Bounded {
    public static void main(String[] args) throws Exception {
        //创建配置环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //获取数据
        DataStreamSource<String> input = env.readTextFile("input");

        //对数据进行压平操作
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDStream = input.flatMap(new Flink01_wordCount_Batch.MyFlatMapper());

        //分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyByDStream = wordToOneDStream.keyBy(0);

        //聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToSumDStream = keyByDStream.sum(1);

        //打印
        wordToSumDStream.print();

        //因为是流式计算需要开启,不需要手动结束，它会自动结束
        env.execute("Flink02_WordCount_Bounded");
    }
}
