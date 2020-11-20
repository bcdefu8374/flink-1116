package com.my.day02;

import com.my.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Collections;
import java.util.Properties;

/**
 * @author chen
 * @topic
 * @create 2020-11-18
 */
public class Test02_split {
    public static void main(String[] args) throws Exception {
        //1.设置流式环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // kafka配置项
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        // 从kafka读取数据
        DataStream<String> textFile = env.addSource( new FlinkKafkaConsumer011<String>("sensor", new SimpleStringSchema(), properties));


        //3.对数据进行切分
        SingleOutputStreamOperator<SensorReading> mapTextFile = textFile.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                //对数据进行切分
                String[] split = value.split(",");
                return new SensorReading(split[0],
                        Long.parseLong(split[1]),
                        Double.parseDouble(split[2]));
            }
        });

        //4.对数据进行判断
        SplitStream<SensorReading> splitStream = mapTextFile.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading value) {
                //给一个标志位

                if (value.getTmp() > 30) {
                    return Collections.singletonList("high");
                } else {
                    return Collections.singletonList("low");
                }

            }
        });

        //5.对数据进行转化
        DataStream<SensorReading> high = splitStream.select("high");
        DataStream<SensorReading> low = splitStream.select("low");


        //打印
        high.print("high");
        low.print("low");

        //5.执行
        env.execute("Test02_split");

    }
}
