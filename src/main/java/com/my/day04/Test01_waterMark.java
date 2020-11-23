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
 * @topic 3.使用事件时间处理数据, 编写代码从端口获取数据实现每隔5秒钟计算最近30秒的每个传感器发送温度的次数,window开窗
 *          Watermark设置延迟2秒钟,允许迟到数据2秒钟,再迟到的数据放置侧输出流。
 *          //允许迟到2s
 *          //watermark2s
 *          如果数据时间是202 那它属于200-205的窗口
 *          //由于迟到和watermark，也就在207的时候触发计算，209的时候关闭窗口
 * @create 2020-11-21
 */
public class Test01_waterMark {
    public static void main(String[] args) throws Exception {
        //1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置创建时间
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.从端口获取数据,并且设置告诉系统哪一个作为时间字段
        SingleOutputStreamOperator<String> input = env.socketTextStream("hadoop102", 7777)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String>() {
            @Override
            public long extractAscendingTimestamp(String element) {
                //对数据进行切分，使用哪一个作为时间字段
                String[] split = element.split(",");
                return Long.parseLong(split[1]) * 1000L;
            }
        });

        //3.压平操作
        SingleOutputStreamOperator<Tuple2<String, Integer>> mapDStream = input.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                //对数据进行切分
                String[] split = value.split(",");
                return new Tuple2<>(split[0], 1);
            }
        });

        //4.分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyByStream = mapDStream.keyBy(0);

        //5.开窗
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> sideOutPut = keyByStream.timeWindow(Time.seconds(30), Time.seconds(5))
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(new OutputTag<Tuple2<String, Integer>>("sideOutPut"){});

        //6.聚合计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = sideOutPut.sum(1);

        //6.1打印
        sum.print("main");

        //6.2获取侧输出流
        DataStream<Tuple2<String, Integer>> sideOut = sum.getSideOutput(new OutputTag<Tuple2<String, Integer>>("sideOutput") {
        });
        //6.3打印侧输出流
        sideOut.print("sideOutPut");


        //7.执行
        env.execute();
    }
}
