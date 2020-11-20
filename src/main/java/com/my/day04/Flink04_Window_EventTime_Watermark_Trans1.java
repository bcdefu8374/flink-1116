package com.my.day04;

import com.my.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @author chen
 * @topic
 * @create 2020-11-20
 */
public class Flink04_Window_EventTime_Watermark_Trans1 {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        //指定使用事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.读取端口数据
        //DataStreamSource<String> input = env.readTextFile("input");
        SingleOutputStreamOperator<String> input = env.socketTextStream("hadoop102", 7777)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(String element) {
                String[] fields = element.split(",");
                return Long.parseLong(fields[1]) * 1000L;
            }
        });

        //3.将数据转化为javaBean
        SingleOutputStreamOperator<SensorReading> map = input.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                //数
                String[] split = value.split(",");
                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });

        //4.重分区
        KeyedStream<SensorReading, Tuple> keyBy = map.keyBy("id");

        //开窗
        WindowedStream<SensorReading, Tuple, TimeWindow> timewindow = keyBy.timeWindow(Time.seconds(5));

        //5.计算
        SingleOutputStreamOperator<SensorReading> sum = timewindow.sum("tmp");

        //6.打印
        sum.print();

        //7.执行
        env.execute("Flink04_Window_EventTime_Watermark_Trans1");


    }
}
