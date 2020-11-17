package com.my.day02;

import com.my.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author chen
 * @topic
 * @create 2020-11-17
 */
public class Transform01_Map {
    public static void main(String[] args) throws Exception {
        //1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从文件读取数据
        DataStreamSource<String> readTextDS = env.readTextFile("input/sensor.txt");

        //3.转化
        SingleOutputStreamOperator<SensorReading> mapTransForm = readTextDS.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                //对数据进行切分
                String[] sensortext = value.split(",");
                return new SensorReading(sensortext[0],
                        Long.parseLong(sensortext[1]),
                        Double.parseDouble(sensortext[2]));
            }
        });

        //打印
        mapTransForm.print();

        //5.开启
        env.execute("Transform_Map");
    }
}
