package com.my.day02;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author chen
 * @topic
 * @create 2020-11-17
 */
public class Transform03_Filter {
    public static void main(String[] args) throws Exception {
        //1.创建读取流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.设置并行度
        env.setParallelism(1);

        //3.读取数据
        DataStreamSource<String> readTextDS = env.readTextFile("input/sensor.txt");

        //4.过滤数据
        SingleOutputStreamOperator<String> filter = readTextDS.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                //获取value
                String[] split = value.split(",");

                if (Double.parseDouble(split[2]) > 30) {
                    return true;
                }

                return false;
            }
        });

        filter.print();

        //5.开启
        env.execute("Transform03_Filter");


    }
}
