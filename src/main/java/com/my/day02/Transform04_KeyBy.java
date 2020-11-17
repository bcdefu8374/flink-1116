package com.my.day02;

import com.my.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
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
public class Transform04_KeyBy {
    public static void main(String[] args) throws Exception {
        //1.创建数据流读取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.设置并行度
        env.setParallelism(1);

        //3.从文件获取数据
        DataStreamSource<String> readTextDS = env.readTextFile("input/sensor.txt");

        //4.把每一行的数据转化为javabean
        SingleOutputStreamOperator<SensorReading> sensonDS = readTextDS.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                //对数据进行切分
                String[] split = value.split(",");
                return new SensorReading(split[0],
                        Long.parseLong(split[1]),
                        Double.parseDouble(split[2]));
            }
        });

        //对数据进行分组，每个组具有相同的key
        KeyedStream<SensorReading, Tuple> keyByStr = sensonDS.keyBy("id");

      /*  SingleOutputStreamOperator<SensorReading> maxDS = keyByStr.max("tmp");
        maxDS.print("max");
*/

        SingleOutputStreamOperator<SensorReading> maxByDS = keyByStr.maxBy("tmp");
        maxByDS.print("maxBy");

        //5.开启
        env.execute("Transform04_KeyBy");


    }
}
