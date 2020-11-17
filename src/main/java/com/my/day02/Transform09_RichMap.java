package com.my.day02;

import com.my.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author chen
 * @topic
 * @create 2020-11-17
 */
public class Transform09_RichMap {
    public static void main(String[] args) throws Exception {
        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //获取数据从文件中
        DataStreamSource<String> textFile = env.readTextFile("input/sensor.txt");

        //获取的数据进行变化
        SingleOutputStreamOperator<SensorReading> sensorDS = textFile.map(new MyRichMap());

        //使用富有函数打印
        sensorDS.print();

        //执行
        env.execute("Transform09_RichMap");

    }



    //创建富有函数
    public static class MyRichMap extends RichMapFunction<String,SensorReading> {

        @Override
        public void open(Configuration parameters) throws Exception {
            //创建连接
            super.open(parameters);
            System.out.println("open方法被调用");
        }

        @Override
        public SensorReading map(String value) throws Exception {
            String[] split = value.split(",");
            return new SensorReading(split[0],
                    Long.parseLong(split[1]),
                    Double.parseDouble(split[2]));
        }

        @Override
        public void close() throws Exception {
            //关闭连接
            super.close();
        }
    }
}
