package com.my.day07;

import com.my.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author chen
 * @topic
 * @create 2020-11-24
 */
public class FlinkSQL07_ProcessTime_Over {
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

        //6.将流转化为表
        Table table = tableEnv.fromDataStream(sensorDStream, "id,ts,tmp,pt.proctime");

        //TableAPI的处理时间，按照时间
//        Table overWindow = table.window(Over.partitionBy("id").orderBy("pt").as("ow"))
//                .select("id,id.count over ow");

//        Table overWindow = table.window(Over.partitionBy("id").orderBy("pt").preceding("3.rows").as("ow"))
//                .select("id,id.count over ow");

        //7.sql
        tableEnv.createTemporaryView("sensor",table);
        Table sqlResult = tableEnv.sqlQuery("select id,count(id) over (partition by id order by pt) as ct from sensor");

        //转化为流输出
        tableEnv.toAppendStream(sqlResult, Row.class).print();

        //执行
        env.execute();
    }
}
