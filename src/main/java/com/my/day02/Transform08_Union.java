package com.my.day02;

import com.my.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;

/**
 * @author chen
 * @topic
 * @create 2020-11-17
 */
public class Transform08_Union {
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

        //6.有split一定要有select
        DataStream<SensorReading> high = splitStr.select("high");
        DataStream<SensorReading> low = splitStr.select("low");

        //7.union连接两个流
        DataStream<SensorReading> union = high.union(low);

        //打印
        union.print();

        //执行
        env.execute("Transform08_Union");
    }
}
