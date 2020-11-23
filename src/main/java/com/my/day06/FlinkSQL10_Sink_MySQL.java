package com.my.day06;

import com.my.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @author chen
 * @topic
 * @create 2020-11-23
 */
public class FlinkSQL10_Sink_MySQL {
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

        //4.创建table执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //将数据流转化为table
        Table table = tableEnv.fromDataStream(sensorDStream);
        //tableAPI
        Table tableResult = table.groupBy("id").select("id,id.count as ct");

        //5.将数据写入Mysql
        String sinkDDL = "create table jdbcOutputTable (" +
                " id varchar(20) not null, " +
                " ct bigint not null " +
                ") with (" +
                " 'connector.type' = 'jdbc', " +
                " 'connector.url' = 'jdbc:mysql://hadoop102:3306/test', " +
                " 'connector.table' = 'sensor_count', " +
                " 'connector.driver' = 'com.mysql.jdbc.Driver', " +
                " 'connector.username' = 'root', " +
                " 'connector.password' = '123456', " +
                " 'connector.write.flush.max-rows' = '1')";
        //更新mysql
        tableEnv.sqlUpdate(sinkDDL);
        tableEnv.insertInto("jdbcOutputTable",tableResult);

        //执行
        env.execute();

    }
}
