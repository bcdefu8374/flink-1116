package com.my.day02;

import com.my.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Collections;

/**
 * @author chen
 * @topic
 * @create 2020-11-17
 */
public class Transform07_Connect {
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

        //5.把流拆成两个数据 流
        SplitStream<SensorReading> splitStr = sensorDS.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading value) {
                if (value.getTmp() > 30) {
                    return Collections.singletonList("high");
                } else {
                    return Collections.singletonList("low");
                }
            }
        });

        //6.选择流
        SingleOutputStreamOperator<Tuple2<String, Double>> high = splitStr.select("high").map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(SensorReading value) throws Exception {
                return new Tuple2<>(value.getId(), value.getTmp());
            }
        });
        DataStream<SensorReading> low = splitStr.select("low");

        //7.连接两个流
        ConnectedStreams<Tuple2<String, Double>, SensorReading> connect = high.connect(low);

        //8.将2个流真正合并
        SingleOutputStreamOperator<Object> map = connect.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
            @Override
            public Object map1(Tuple2<String, Double> value) throws Exception {
                return new Tuple3<String, Double, String>(value.f0, value.f1, "warn");
            }

            @Override
            public Object map2(SensorReading value) throws Exception {
                return new Tuple2<String, String>(value.getId(), "healthy");
            }
        });

        //打印
        map.print();

        //执行
        env.execute("Transform07_Connect");

    }
}
