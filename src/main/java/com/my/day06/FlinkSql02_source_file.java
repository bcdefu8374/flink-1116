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
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

/**
 * @author chen
 * @topic
 * @create 2020-11-23
 */
public class FlinkSql02_source_file {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(1);

        //3.创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //4.定义文件连接器
        tableEnv.connect(new FileSystem().path("input/sensor.txt"))
                .withFormat(new OldCsv())
                .withSchema(new Schema()
                    .field("id", DataTypes.STRING())
                    .field("ts",DataTypes.BIGINT())
                    .field("tmp",DataTypes.DOUBLE()))
                .createTemporaryTable("fileInput");

        //4.创建一张表读取数据
        Table table = tableEnv.from("fileInput");

        //5.TableAPI
        Table tableResult = table.where("id = 'sensor_1'").select("id,tmp");

        //6.sql
        Table sqlResult = tableEnv.sqlQuery("select id,tmp from fileInput where id = 'sensor_1'");

        //7.将结果打印出来
        tableEnv.toAppendStream(tableResult, Row.class).print("tableResult");
        tableEnv.toAppendStream(sqlResult,Row.class).print("sqlResult");
        //9.执行
        env.execute();
    }
}
