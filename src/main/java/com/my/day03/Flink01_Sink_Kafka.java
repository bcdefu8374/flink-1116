package com.my.day03;


import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

/**
 * @author chen
 * @topic  kafka充当消费者
 * @create 2020-11-18
 */
public class Flink01_Sink_Kafka {
    public static void main(String[] args) throws Exception {
        //1.获取数据流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.获取数据
        DataStreamSource<String> textFile = env.readTextFile("input/sensor.txt");

        //3.将数据写入kafka
        textFile.addSink(new FlinkKafkaProducer011<String>("hadoop102:9092","test",new SimpleStringSchema()));

        //4.执行
        env.execute("Flink01_Sink_Kafka");
    }

}
