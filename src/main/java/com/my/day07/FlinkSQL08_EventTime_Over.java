package com.my.day07;

import com.my.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author chen
 * @topic
 * @create 2020-11-24
 */
public class FlinkSQL08_EventTime_Over {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //使用enentTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.读取端口数据创建流
        DataStreamSource<String> textFile = env.socketTextStream("hadoop102", 7777);

        //创建waterMark
        SingleOutputStreamOperator<String> assignWartermark = textFile.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(String element) {
                //对数据进行切分指定那个字段为时间字段
                String[] split = element.split(",");
                return Long.parseLong(split[1]) * 1000L;
            }
        });

        //3.将每一行数据转化为JavaBean
        SingleOutputStreamOperator<SensorReading> sensorDStream = assignWartermark.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] split = value.split(",");
                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });


        //4.创建table执行环境

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //5.流转化为table
        Table table = tableEnv.fromDataStream(sensorDStream, "id,ts,tmp,rt.rowtime");

        //6.TableAPI
        Table overWindowEvent = table.window(Over.partitionBy("id").orderBy("pt").as("ow"))
                .select("id,id.count as ct");
        Table eventRow = table.window(Over.partitionBy("id").orderBy("pt").preceding("5.rows").as("ow"))
                .select("id,id.count as ct");

        //7.sql
//        tableEnv.createTemporaryView("sensor",table);
//        tableEnv.sqlQuery("")

        //转化为流输出
        tableEnv.toAppendStream(overWindowEvent, Row.class).print();


        //8.执行
        env.execute();
    }
}
