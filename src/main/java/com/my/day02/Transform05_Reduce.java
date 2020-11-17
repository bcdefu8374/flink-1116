package com.my.day02;

import com.my.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author chen
 * @topic
 * @create 2020-11-17
 */
public class Transform05_Reduce {
    public static void main(String[] args) throws Exception {
        //1.创建数据流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.设置并行度
        env.setParallelism(1);

        //3.从文件读取数据
        DataStreamSource<String> textFile = env.readTextFile("input/sensor.txt");

        //4.数据转化为javaBean
        SingleOutputStreamOperator<SensorReading> sensorDS = textFile.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {

                //读数据 进行切分
                String[] split = value.split(",");
                return new SensorReading(split[0],
                        Long.parseLong(split[1]),
                        Double.parseDouble(split[2]));
            }
        });

        //5.对数据进行分组
        KeyedStream<SensorReading, Tuple> keyByDS = sensorDS.keyBy("id");

        //6.对数据进行聚合
        SingleOutputStreamOperator<SensorReading> reduceDS = keyByDS.reduce(new ReduceFunction<SensorReading>() {
            @Override
            public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
                return new SensorReading(value1.getId(),
                        value2.getTs(),
                        Math.max(value1.getTmp(), value2.getTmp()));
            }
        });

        reduceDS.print("reduce");

        //5.开启
        env.execute("Transform05_Reduce");
    }
}
