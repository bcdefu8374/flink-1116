package com.my.day03;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author chen
 * @topic
 * @create 2020-11-18
 */
public class Flink06_Window_SlidingTime {
    public static void main(String[] args) throws Exception {
        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //从端口获取数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 7777);

        //3.对 数据进行分组
        SingleOutputStreamOperator<Tuple2<String, Integer>> tupleFlatMap = socketTextStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                //对数据 进行切分
                String[] split = value.split(" ");
                //2.遍历数据
                for (String word : split) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        });

        KeyedStream<Tuple2<String, Integer>, Tuple> keyByStream = tupleFlatMap.keyBy(0);

        //4.对分组后数据进行开窗
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> timeWindow = keyByStream.timeWindow(Time.seconds(5),Time.seconds(2));

        //5.对开完窗的数据进行聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = timeWindow.sum(1);

        //6.打印
        sum.print();

        //7.执行
        env.execute("Flink05_Window_TumplingTime");

    }
}
