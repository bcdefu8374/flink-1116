package com.my.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author chen
 * @topic  批处理wordcount
 * @create 2020-11-16
 */
public class Flink01_wordCount_Batch {
    public static void main(String[] args) throws Exception {
        //1.创建flink环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //2.从文本读取数据
        DataSource<String> line = env.readTextFile("input/1.txt");

        //3.按照空格进行打散
        FlatMapOperator<String, Tuple2<String, Integer>> wordToOneDSet = line.flatMap(new MyFlatMapper());

        //6.对数据进行分组
        UnsortedGrouping<Tuple2<String, Integer>> groupByDStream = wordToOneDSet.groupBy(0);

        //7.聚合
        AggregateOperator<Tuple2<String, Integer>> wordToSum = groupByDStream.sum(1);

        //打印
        wordToSum.print();

    }

    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String,Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            //4.获取数据进行切分
            String[] lineSplit = value.split(" ");

            //5.遍历数据
            for (String word : lineSplit) {
                out.collect(new Tuple2<>(word,1));
            }
        }
    }
}
