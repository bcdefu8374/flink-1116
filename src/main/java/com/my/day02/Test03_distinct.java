package com.my.day02;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;


/**
 * @author chen
 * @topic
 * @create 2020-11-18
 */
public class Test03_distinct {
    public static void main(String[] args) throws Exception {
        //1.设置流式环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        //2.从文本文件读取数据
        DataStreamSource<String> textFile = env.readTextFile("input/1.txt");

        //3.压平操作
        SingleOutputStreamOperator<String> flatMapDS = textFile.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                //对数据进行切分
                String[] split = value.split(" ");

                //遍历数据
                for (String word : split) {
                    out.collect(word);
                }

            }
        });

        //4.过滤
        SingleOutputStreamOperator<String> filter = flatMapDS.filter(new MyRichFilter());


        //6.打印
        filter.print();

        //5.开启
        env.execute("Test03_distinct");
    }

    public static class MyRichFilter extends RichFilterFunction<String>{

        //定义redis
        Jedis jedis = null;

        //设置rowkey
        String rowkey = "distinct";


        @Override
        public void open(Configuration parameters) throws Exception {
            //获取redis连接
            Jedis jedis = new Jedis("hadoop102,6379");
        }

        @Override
        public boolean filter(String value) throws Exception {
            //在redis中是否存在该单词
            Boolean exist = jedis.sismember(rowkey, value);
            //如果不存在就写入redis
            if (!exist){
                jedis.sadd(rowkey,value);
            }
            return !exist;
        }

        @Override
        public void close() throws Exception {
            jedis.close();
        }
    }
}
