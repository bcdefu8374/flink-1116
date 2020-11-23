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
public class FlinkSql01_test {
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

        //4.创建TableAPI执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //5.从流中创建表
        Table table = tableEnv.fromDataStream(sensorDStream);

        //6.转化数据
        //6.1使用tableAPI转换数据
        Table result = table.select("id,tmp").filter("id = 'sensor_1'");

        //6.2使用flinksql转化数据
        tableEnv.createTemporaryView("sensor",sensorDStream);
        Table sqlResult = tableEnv.sqlQuery("select id,tmp from sensor where id='sensor_1'");

        //7.转换为流输出数据
        tableEnv.toAppendStream(result, Row.class).print("result");
        tableEnv.toAppendStream(sqlResult, Row.class).print("sqlResult");

        //8.启动任务
        env.execute();


    }
}
