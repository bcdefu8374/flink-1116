package com.my.day01;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author chen
 * @topic  无界流wordCount
 * @create 2020-11-16
 */
public class Flink03_WordCount_Unbounded {
    public static void main(String[] args) throws Exception {
        //1.创建配置环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.获取端口信息
        DataStreamSource<String> portDStream = env.socketTextStream("hadoop102", 7777);
        //3.对传过来的数据进行压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = portDStream.flatMap(new Flink01_wordCount_Batch.MyFlatMapper());

        //4.分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyByWord = wordToOne.keyBy(0);

        //5.聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToSum = keyByWord.sum(1);

        //5.打印
        wordToSum.print();

        //6.开启
        env.execute();
    }
}
