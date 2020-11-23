package com.my.day06;

import com.my.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author chen
 * @topic
 * @create 2020-11-23
 */
public class FlinkSQL05_Agg {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境并设置并行度
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.读取文本数据创建流
        DataStreamSource<String> textFile = env.readTextFile("input/sensor.txt");

        //3.将每一行数据转化为JavaBean
        SingleOutputStreamOperator<SensorReading> sensorDStream = textFile.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] split = value.split(",");
                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });

        //4.创建table执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //5.使用Table执行环境将流转化为Table
        tableEnv.createTemporaryView("sensor", sensorDStream);

        //6.TableAPI
        Table table = tableEnv.from("sensor");
        Table tableResult = table.groupBy("id").select("id,id.count,tmp.avg");

        //7.sql
        Table sqlResult = tableEnv.sqlQuery("select id,count(id) from sensor group by id");

        //8.打印
        tableEnv.toRetractStream(tableResult, Row.class).print("tableResult");
        tableEnv.toRetractStream(sqlResult, Row.class).print("sqlResult");

        //9.执行
        env.execute();
    }
}
