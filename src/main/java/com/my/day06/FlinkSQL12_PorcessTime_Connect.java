package com.my.day06;

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
public class FlinkSQL12_PorcessTime_Connect {
    public static void main(String[] args) {

        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //创建table执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //读取端口数据创建流，转换为JavaBean
        tableEnv.connect(new FileSystem().path("sensor"))
                .withSchema(new Schema()
                .field("id", DataTypes.STRING())
                .field("ts",DataTypes.BIGINT())
                .field("tmp",DataTypes.DOUBLE())
                .field("pt",DataTypes.TIMESTAMP(3)).proctime())
                .withFormat(new OldCsv())
                .createTemporaryTable("fileInput");

        //打印schema的数据
        Table fileInput = tableEnv.from("fileInput");
        fileInput.printSchema();

    }
}
