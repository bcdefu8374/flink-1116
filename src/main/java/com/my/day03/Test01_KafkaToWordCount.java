package com.my.day03;

import com.my.day01.Flink01_wordCount_Batch;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

/**
 * @author chen
 * @topic   读取Kafka test1主题的数据计算WordCount存入MySQL.
 * @create 2020-11-20
 */
public class Test01_KafkaToWordCount {
    public static void main(String[] args) throws Exception {
        //获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.设置并行度
        env.setParallelism(1);

        //2.从kafka获取数据
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        DataStreamSource<String> test1Kafka = env.addSource(new FlinkKafkaConsumer011<String>("test1",
                new SimpleStringSchema(),
                properties));


        //2.计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMapDS = test1Kafka.flatMap(new Flink01_wordCount_Batch.MyFlatMapper());

        //3.分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyByWords = flatMapDS.keyBy(0);

        //4.聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumDS = keyByWords.sum(1);

        //3.获取Jdbc连接
        sumDS.addSink(new JdbcToMysqlSink());


        //5.打印
        sumDS.print();

        //7.执行
        env.execute("Test01_KafkaToWordCount");
    }

    public static class JdbcToMysqlSink extends RichSinkFunction<Tuple2<String, Integer>>{
        //声明mysql相关属性信息
        Connection connection = null;
        PreparedStatement preparedStatement = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            //获取连接
            connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test", "root", "123456");
            //获取连接写sql
            preparedStatement = connection.prepareStatement("INSERT INTO wordcount(word,count) VALUES(?,?) ON DUPLICATE KEY UPDATE count=? ");
        }

//        @Override
//        public void invoke(String value, Context context) throws Exception {
//
//            //1.分割数据
//            String[] split = value.split(",");
//            preparedStatement.setString(1,split[0]);
//            preparedStatement.setDouble(2,Double.parseDouble(split[2]));
//            preparedStatement.setDouble(3,Double.parseDouble(split[2]));
//            //2.执行
//            preparedStatement.execute();
//        }


        @Override
        public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {

            preparedStatement.setString(1,value.f0);
            preparedStatement.setInt(2,value.f1);
            preparedStatement.setInt(3,value.f1);

            //2.执行
            preparedStatement.execute();
        }

        @Override
        public void close() throws Exception {
            connection.close();
            preparedStatement.close();
        }
    }

}
