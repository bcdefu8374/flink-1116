package com.my.day06;

import com.my.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * @author chen
 * @topic
 * @create 2020-11-23
 */
public class FlinkSQL07_Sink_Kafka {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        //1.获取执行环境并设置并行度
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.读取端口数据创建流
        DataStreamSource<String> textFile = env.socketTextStream("hadoop102", 7777);

        //3.将每一行数据转化为JavaBean
        SingleOutputStreamOperator<SensorReading> sensorDStream = textFile.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] split = value.split(",");
                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });


        //创建table执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //4.将流转化为表Table，用于tableAPI
        Table table = tableEnv.fromDataStream(sensorDStream);
        Table tableResult = table.select("id,tmp").where("id = 'sensor_1'");

        //使用sql
        tableEnv.createTemporaryView("socket",sensorDStream);
        Table sqlResult = tableEnv.sqlQuery("select id,tmp from socket where id='sensor_1'");

        //将数据写入kafka
        tableEnv.connect(new Kafka()
        .version("0.11")
        .topic("test")
        .property(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                .field("id", DataTypes.STRING())
                .field("tmp",DataTypes.DOUBLE()))
                .createTemporaryTable("kafkaSink");
        sqlResult.insertInto("kafkaSink");

        //7.执行任务
        env.execute();

    }
}
