package com.my.day02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author chen
 * @topic
 * @create 2020-11-18
 */
public class Test01_WordCount {
    public static void main(String[] args) throws Exception {
        //1.设置流式环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.从文本读取数据
        DataStreamSource<String> textFile = env.socketTextStream("hadoop102",7777);

        //3.对数据进行切分
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = textFile.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                //读数据进行切分
                String[] split = value.split(" ");

                //读数据遍历
                for (String word : split) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        });

        //4.读数据进行分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyByDS = wordToOne.keyBy(0);

        //5.读分组后的数据进行聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> reduceDS = keyByDS.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
            }
        });


        //6.打印
        reduceDS.print("reduce_wordcount");

        //5.开启
        env.execute("Transform10_WordCount");
    }
}
