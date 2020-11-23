package com.my.day06;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;

/**
 * @author chen
 * @topic
 * @create 2020-11-23
 */
public class FlinkSQL15_EventTime_Connect {
    public static void main(String[] args) {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.创建文件连接器
        tableEnv.connect(new FileSystem().path("sensor"))
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("ts", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE())
                        .field("rt", DataTypes.BIGINT()).rowtime(new Rowtime()
                                .timestampsFromField("ts")
                                .watermarksPeriodicBounded(1000))
                )
                .withFormat(new Csv())
                .createTemporaryTable("fileInput");

        //3.打印schema信息
        Table table = tableEnv.from("fileInput");
        table.printSchema();
    }
}
