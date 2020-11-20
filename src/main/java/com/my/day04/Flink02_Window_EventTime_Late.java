package com.my.day04;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.OutputTag;

/**
 * @author chen
 * @topic
 * @create 2020-11-20
 */
public class Flink02_Window_EventTime_Late {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //指定使用事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //读取端口数据
        SingleOutputStreamOperator<String> input = env.socketTextStream("hadoop102", 7777)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String>() {
            @Override
            public long extractAscendingTimestamp(String element) {
                //对数据进行切分
                String[] split = element.split(",");
                //返回数据
                return Long.parseLong(split[1]) * 10000L;
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

        //4.重分区
        KeyedStream<Tuple2<String, Integer>, Tuple> keyBy = map.keyBy(0);

        //5.开窗
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> windowed = keyBy.timeWindow(Time.seconds(5))  //开窗时间5s
                .allowedLateness(Time.seconds(2))   //允许迟到2s，
                .sideOutputLateData(new OutputTag<Tuple2<String, Integer>>("sideOutPut") {  //如果迟到2s还没有到就放到侧输出流
        });

        //6.计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = windowed.sum(1);

        //7.打印
        sum.print("main");

        //7.2获取侧输出流数据
        //放进侧输出流，也得把它拿出来放到侧输出流
        DataStream<Tuple2<String, Integer>> sideOutput = sum.getSideOutput(new OutputTag<Tuple2<String, Integer>>("sideOutPut") {
        });
        sideOutput.print("sideOutPut");

        //8.执行
        env.execute("Flink02_Window_EventTime_Late ");

    }
}
