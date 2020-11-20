package com.my.day04;

import com.my.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @author chen
 * @topic
 * @create 2020-11-20
 */
public class Flink05_Window_EventTime_Watermark_Trans2 {
    public static void main(String[] args) throws Exception {
        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(1);
        //设置Event.time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //2.从端口获取数据
        DataStreamSource<String> sorketText = env.socketTextStream("hadoop102", 7777);

        //3.获取的数据转化为javaBean
        SingleOutputStreamOperator<SensorReading> wordBean = sorketText.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                //对数据进行切分
                String[] split = value.split(",");
                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });

        //4.告诉系统哪一个作为判定的时间字段
        SingleOutputStreamOperator<SensorReading> sensor = wordBean.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(SensorReading element) {
                //对数据进行切分判断使用哪个字段作为时间字段
                return element.getTs() * 1000L;
            }
        });

        //5.分组开窗
        WindowedStream<SensorReading, Tuple, TimeWindow> timeWindow = sensor.keyBy("id").timeWindow(Time.seconds(5));

        //聚合
        SingleOutputStreamOperator<SensorReading> tmp = timeWindow.sum("temperature");
        //6.打印
        tmp.print();

        //7.执行
        env.execute("Flink05_Window_EventTime_Watermark_Trans2");
    }
}
