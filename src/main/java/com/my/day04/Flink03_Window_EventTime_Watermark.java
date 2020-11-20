package com.my.day04;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.OutputTag;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

/**
 * @author chen
 * @topic
 * @create 2020-11-20
 */
public class Flink03_Window_EventTime_Watermark {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.指定事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //3.从端口获取数据
        SingleOutputStreamOperator<String> input = env.socketTextStream("hadoop102", 7777)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(5)) {
                    @Override
                    public long extractTimestamp(String element) {
                        String[] split = element.split(",");
                        return Long.parseLong(split[1]) * 1000L;
                    }
                });

        //4.压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> map = input.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] split = value.split(",");
                return new Tuple2<>(split[0], 1);
            }
        });

        //5.重分区
        KeyedStream<Tuple2<String, Integer>, Tuple> keyBy = map.keyBy(0);

        //6.开窗
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> windowTime = keyBy.timeWindow(Time.seconds(5))
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(new OutputTag<Tuple2<String, Integer>>("sideOutPut") {
                });

        //7.计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = windowTime.sum(1);

        //8.打印
        sum.print("main");

        //9.输出流数据
        DataStream<Tuple2<String, Integer>> sideOutput = sum.getSideOutput(new OutputTag<Tuple2<String, Integer>>("sideOutPut") {
        });
        sideOutput.print("sideOutPut");

        //10.执行任务
        env.execute("Flink03_Window_EventTime_Watermark");

    }
}
