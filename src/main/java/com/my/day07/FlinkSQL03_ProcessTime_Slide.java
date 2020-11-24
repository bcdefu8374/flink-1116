package com.my.day07;

import com.my.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author chen
 * @topic
 * @create 2020-11-24
 */
public class FlinkSQL03_ProcessTime_Slide {
    public static void main(String[] args) throws Exception {
        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //创建table执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.从端口读取数据转化为JavaBean
        DataStreamSource<String> textStream = env.socketTextStream("hadoop102", 7777);

        SingleOutputStreamOperator<SensorReading> sensorDStream = textStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {

                //对数据进行切分
                String[] split = value.split(",");
                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });

        //将流转化为为表
        Table table = tableEnv.fromDataStream(sensorDStream, "id,ts,tmp,pt.proctime");

        //使用TableAPI处理时间的 按照时间
//        Table slidesecondes = table.window(Slide.over("5.seconds").every("2.seconds").on("pt").as("sw"))
//                .groupBy("id,sw")
//                .select("id,id.count");
//
        //使用TableAPI处理时间的 按照行

//        Table slidesecondes = table.window(Slide.over("5.rows").every("2.rows").on("pt").as("sw"))
//                .groupBy("id,sw")
//                .select("id,id.count");

        //使用sql
        tableEnv.createTemporaryView("sensor",table);
        Table sqlResult = tableEnv.sqlQuery("select id,count(id) as ct from sensor group by id,hop(pt,INTERVAL '2' second,INTERVAL '6' second)");


        //输出
        tableEnv.toAppendStream(sqlResult, Row.class).print();


        //8.执行
        env.execute();

    }
}
