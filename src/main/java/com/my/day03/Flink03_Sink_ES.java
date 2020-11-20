package com.my.day03;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author chen
 * @topic
 * @create 2020-11-18
 */
public class Flink03_Sink_ES {
    public static void main(String[] args) throws Exception {
        //1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.从文本读取数据
        DataStreamSource<String> textFile = env.readTextFile("input/sensor.txt");

        //3.把数据写到es
        //3.1创建集合用于存放连接条件
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("hadoop102",9200));

        //4.构建ES
        ElasticsearchSink<String> build = new ElasticsearchSink.Builder<String>(httpHosts, new MyEsSinkFun()).build();
        textFile.addSink(build);

        //4.执行
        env.execute(" Flink03_Sink_ES");
    }

    public static class MyEsSinkFun implements ElasticsearchSinkFunction<String>{

        @Override
        public void process(String element, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {

            //1.对数据进行切分
            String[] split = element.split(",");

            HashMap<String, String> source = new HashMap<>();
            source.put("id",split[0]);
            source.put("ts",split[1]);
            source.put("tmp",split[2]);

            //2.返回es信息
            IndexRequest indexReque = Requests.indexRequest().index("sensor").type("_doc").source(source);

            //3.将数据写入
            requestIndexer.add(indexReque);

        }
    }
}
