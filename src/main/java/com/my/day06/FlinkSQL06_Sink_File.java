package com.my.day06;

import com.my.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;

/**
 * @author chen
 * @topic
 * @create 2020-11-23
 */
public class FlinkSQL06_Sink_File {
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

        //2.将流转化为表
        Table table = tableEnv.fromDataStream(sensorDStream);

        //3.TableAPI
        Table tableResult = table.groupBy("id").select("id,id.count as ct");

        //4.SQL
        tableEnv.createTemporaryView("socket",sensorDStream);
        Table sqlResult = tableEnv.sqlQuery("select id,tmp from socket where id = 'sensor_1'");


        //5.将数据写入文件
        tableEnv.connect(new FileSystem().path("sensorOut2"))
                .withFormat(new OldCsv())
                .withSchema(new Schema()
                .field("id", DataTypes.STRING())
                .field("tmp",DataTypes.DOUBLE()))
                .createTemporaryTable("sensorOut2");

        //输出路径
        tableEnv.insertInto("sensorOut2",sqlResult);



        //9.执行任务
        env.execute();
    }
}
