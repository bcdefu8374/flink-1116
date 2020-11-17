package com.my.day02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author chen
 * @topic
 * @create 2020-11-17
 */
public class Transform02_FlatMap {
    public static void main(String[] args) throws Exception {
        //1.创建
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.设置并行度
        env.setParallelism(1);

        //3.读取 数据
        DataStreamSource<String> readTextDS = env.readTextFile("input/sensor.txt");

        //4.压平数据操作
        SingleOutputStreamOperator<Object> flatMapSO = readTextDS.flatMap(new FlatMapFunction<String, Object>() {
            @Override
            public void flatMap(String value, Collector<Object> out) throws Exception {
                //切分
                String[] sensors = value.split(",");

                //遍历数据
                for (String tem : sensors) {
                    out.collect(tem);
                }
            }
        });

        flatMapSO.print();


        //5.开启
        env.execute("Transform_FlatMap");
    }
}
