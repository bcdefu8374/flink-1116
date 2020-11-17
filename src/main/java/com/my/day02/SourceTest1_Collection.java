package com.my.day02;

import com.my.bean.SensorReading;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author chen
 * @topic  从集合中读取数据，有界流
 * @create 2020-11-17
 */
public class SourceTest1_Collection {
    public static void main(String[] args) throws Exception {
        //1.获取环境，批处理使用ExecutionEnvironment，
       //流处理使用 StreamExecutionEnvironment,流处理需要开启，因为一直等在哪里，如果使用批处理就不需要使用开启
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.设置并行度
        env.setParallelism(1);

        //3.从集合中读取数据
        DataStream<SensorReading> arrReadDS = env.fromCollection(Arrays.asList(
                new SensorReading("sensor_1", 1547718199L, 35.8),
                new SensorReading("sensor_6", 1547718201L, 15.4),
                new SensorReading("sensor_7", 1547718202L, 6.7),
                new SensorReading("sensor_10", 1547718205L, 38.1)
        ));

        //4.打印
        arrReadDS.print();

        //5.开启
        env.execute("SourceTest1_Collection");


       //批处理
        /*ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        DataSource<SensorReading> arrReadDS = env.fromCollection(Arrays.asList(
                new SensorReading("sensor_1", 1547718199L, 35.8),
                new SensorReading("sensor_6", 1547718201L, 15.4),
                new SensorReading("sensor_7", 1547718202L, 6.7),
                new SensorReading("sensor_10", 1547718205L, 38.1)
        ));

        arrReadDS.print();*/
    }
}
