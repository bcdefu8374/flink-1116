package com.my.day03;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @author chen
 * @topic
 * @create 2020-11-18
 */
public class Flink02_Sink_Redis {
    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.读取文本信息
        DataStreamSource<String> textFile = env.readTextFile("input/sensor.txt");

        //3.将数据写入redis
        FlinkJedisPoolConfig poolConfig = new FlinkJedisPoolConfig.Builder().setHost("hadoop102").setPort(6379).build();

        //4.使用addsink
        textFile.addSink(new RedisSink<>(poolConfig,new MySinkFunc()));

        //4.执行
        env.execute("Flink02_Sink_Redis");
    }

    public static class MySinkFunc implements RedisMapper<String> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET,"sensor");
        }

        @Override
        public String getKeyFromData(String data) {
            String[] fields = data.split(" ");
            return fields[0];
        }

        @Override
        public String getValueFromData(String data) {
            String[] fields = data.split(" ");
            return fields[2];
        }
    }
}
