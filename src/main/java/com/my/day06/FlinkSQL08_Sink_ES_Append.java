package com.my.day06;

import com.my.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Elasticsearch;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * @author chen
 * @topic
 * @create 2020-11-23
 */
public class FlinkSQL08_Sink_ES_Append {
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

        //1.创建table执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //2.将流转化为table
        Table table = tableEnv.fromDataStream(sensorDStream);
        //3.TableAPI
        Table tableResult = table.select("id,tmp").where("id = 'sensor_1'");

        //4.Sql
        tableEnv.createTemporaryView("socket",sensorDStream);
        Table sqlResult = tableEnv.sqlQuery("select id,tmp from socket where id = 'sensor_1'");

        //5.写入es
        tableEnv.connect(new Elasticsearch()
        .version("6")
        .host("hadoop102",9200,"http")
        .index("flink_sql")
        .documentType("_doc")
        .bulkFlushMaxActions(1))
                .inAppendMode()
                .withFormat(new Json())
        .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("tmp",DataTypes.DOUBLE()))
        .createTemporaryTable("EsPath");

        //插入
        tableEnv.insertInto("EsPath",tableResult);

        tableEnv.toAppendStream(tableResult, Row.class).print();

        //6.执行
        env.execute();

    }
}
