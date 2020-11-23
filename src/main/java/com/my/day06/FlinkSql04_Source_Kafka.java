package com.my.day06;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;

/**
 * @author chen
 * @topic
 * @create 2020-11-23
 */
public class FlinkSql04_Source_Kafka {
    public static void main(String[] args) throws Exception {
        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.创建Kafka连接器
        tableEnv.connect(new Kafka()
        .version("0.11")
        .topic("test")
        .property(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092")
        .property(ConsumerConfig.GROUP_ID_CONFIG,"testKafkaSource"))
                .withFormat(new Json())
                .withSchema(new Schema()
                .field("id", DataTypes.STRING())
                .field("ts",DataTypes.BIGINT())
                .field("tmp",DataTypes.DOUBLE()))
                .createTemporaryTable("kafkaInput");
        //3.创建表
        Table table = tableEnv.from("kafkaInput");

        //4.TableAPI
        Table tableResult = table.where("id = 'sensor_1'").select("id,tmp");

        //5.sql
        Table sqlResult = tableEnv.sqlQuery("select id,tmp from kafkaInput where id = 'sensor_1'");

        //6.将结果数据打印
        tableEnv.toAppendStream(tableResult, Row.class).print("tableResult");
        tableEnv.toAppendStream(sqlResult, Row.class).print("sqlResult");

        //9.执行
        env.execute();
    }
}
