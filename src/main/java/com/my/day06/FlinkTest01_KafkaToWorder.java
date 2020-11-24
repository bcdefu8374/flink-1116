package com.my.day06;

import com.my.bean.SensorReading;
import com.my.bean.WordCount;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @author chen
 * @topic
 * @create 2020-11-24
 */
public class FlinkTest01_KafkaToWorder {

    public static void main(String[] args) throws Exception {

        //获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(1);

        //2.获取table执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        /*//3.创建kafka连接器
        tableEnv.connect(new Kafka()
        .version("0.11")
        .topic("test")
        .property(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092")
        .property(ConsumerConfig.GROUP_ID_CONFIG,"kafkaSource"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                .field("word", DataTypes.STRING())
                .field("count",DataTypes.BIGINT()))
                .createTemporaryTable("kafkaInput");*/

        //3.从kafka读取数据
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        // 从kafka读取数据
        DataStream<String> textFile = env.addSource( new FlinkKafkaConsumer011<String>("test", new SimpleStringSchema(), properties));

        //4.把每一行的数据转化为javabean
        SingleOutputStreamOperator<String> sensorDStream = textFile.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] split = value.split(",");

                for (String word : split) {
                    out.collect(word);
                }
            }
        });

        sensorDStream.print();


        //3.将流转换为表
//        Table table = tableEnv.fromDataStream(sensorDStream,"word");
        Table word = tableEnv.fromDataStream(sensorDStream, "word");


        //使用sql
        tableEnv.createTemporaryView("wordCount", sensorDStream);
        Table sqlResult = tableEnv.sqlQuery("select word,count(word) ct from wordCount group by word");
        sqlResult.printSchema();

        //写入es
        //5.将数据写入ES
        tableEnv.connect(new Elasticsearch()
                .version("6")
                .host("hadoop102", 9200, "http")
                .index("flink_sql02")
                .disableFlushOnCheckpoint()
                .bulkFlushMaxActions(1)
                .documentType("_doc"))
                .inUpsertMode()
                .withFormat(new Json())
                .withSchema(new Schema()
                        .field("word", DataTypes.STRING())
                        .field("ct", DataTypes.BIGINT()))
                .createTemporaryTable("EsPath");

        tableEnv.insertInto("EsPath", sqlResult);

        tableEnv.toRetractStream(sqlResult, Row.class).print();

        //执行
        env.execute();

    }
}
