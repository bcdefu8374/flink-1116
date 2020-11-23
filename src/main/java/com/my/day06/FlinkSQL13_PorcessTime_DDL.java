package com.my.day06;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @author chen
 * @topic
 * @create 2020-11-23
 */
public class FlinkSQL13_PorcessTime_DDL {
    public static void main(String[] args) {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //3.基于新版本的流式处理环境
        EnvironmentSettings baSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(env, baSettings);


        //2.读取端口数据创建流,转换为JavaBean
        String sinkDDL = "create table dataTable (" +
                " id varchar(20) not null, " +
                " ts bigint, " +
                " temp double, " +
                " pt AS PROCTIME() " +
                ") with (" +
                " 'connector.type' = 'filesystem', " +
                " 'connector.path' = 'sensor', " +
                " 'format.type' = 'csv')";
        bsTableEnv.sqlUpdate(sinkDDL);


        //打印消息
        Table dataTable = bsTableEnv.from("dataTable");
        dataTable.printSchema();

    }
}
