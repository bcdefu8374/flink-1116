package com.my.day04;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @author chen
 * @topic  周期性的生成watermark
 * @create 2020-11-20
 */
public class Flink01_Window_EventTime {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //指定使用事件时间,设置时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //读取端口数据
        SingleOutputStreamOperator<String> input = env.socketTextStream("hadoop102", 7777)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String>() {  //提取事件时间，周期性
            @Override
            public long extractAscendingTimestamp(String element) {
                //对数据进行切分
                String[] split = element.split(",");
                //返回数据
                return Long.parseLong(split[1]) * 1000L;  //必须提取时间得用毫秒数，要告诉系统那一个作为时间字段
            }
        });

        //3.压平操作
        SingleOutputStreamOperator<Tuple2<String, Integer>> map = input.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                //对数据进行切分
                String[] split = value.split(",");
                //返回数据
                return new Tuple2<>(split[0], 1);
            }
        });

        //4.重分区，分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyByDStream = map.keyBy(0);

        //4.1开窗按照事件的时间戳为5个就触发，
        // 例如：1547718199 的时间戳是199，那它属于195-200的，并且左闭右开。那它到200才能触发，但是 200这条数据要计算在下一个窗口中
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> timeWindow = keyByDStream.timeWindow(Time.seconds(5));

        //5.聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = timeWindow.sum(1);

        //5.打印
        sum.print();

        //6.执行
        env.execute("Flink01_Window_EventTime");


    }
}
