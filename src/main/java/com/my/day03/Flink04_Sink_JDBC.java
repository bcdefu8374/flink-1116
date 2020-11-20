package com.my.day03;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;


/**
 * @author chen
 * @topic
 * @create 2020-11-18
 */
public class Flink04_Sink_JDBC {
    public static void main(String[] args) throws Exception {
        //1.创建连接环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.从文本获取数据
        DataStreamSource<String> textFile = env.readTextFile("input/sensor.txt");

        //3.获取Jdbc连接
        textFile.addSink(new JdbcSink());

        //4.执行
        env.execute("Flink04_Sink_JDBC");
    }

    private static class JdbcSink extends RichSinkFunction<String> {

        //声明mysql相关属性信息
        Connection connection = null;
        PreparedStatement preparedStatement = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            //获取连接
            connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test", "root", "123456");
            //获取连接写sql
            preparedStatement = connection.prepareStatement("INSERT INTO sensor(id,tmp) VALUES(?,?) ON DUPLICATE KEY UPDATE tmp=? ");
        }

        @Override
        public void invoke(String value, Context context) throws Exception {

            //1.分割数据
            String[] split = value.split(",");
            preparedStatement.setString(1,split[0]);
            preparedStatement.setDouble(2,Double.parseDouble(split[2]));
            preparedStatement.setDouble(3,Double.parseDouble(split[2]));
            //2.执行
            preparedStatement.execute();
        }

        @Override
        public void close() throws Exception {
            connection.close();
            preparedStatement.close();
        }
    }
}
