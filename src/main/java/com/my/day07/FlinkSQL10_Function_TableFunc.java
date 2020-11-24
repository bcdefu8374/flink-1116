package com.my.day07;

import com.my.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @author chen
 * @topic
 * @create 2020-11-24
 */
public class FlinkSQL10_Function_TableFunc {
    public static void main(String[] args) throws Exception {
        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //创建table执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.从端口读取数据转化为JavaBean
        DataStreamSource<String> textStream = env.socketTextStream("hadoop102", 7777);

        SingleOutputStreamOperator<SensorReading> sensorDStream = textStream.map((MapFunction<String, SensorReading>) value -> {

            //对数据进行切分
            String[] split = value.split(",");
            return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
        });

        //将流转化为表
        Table table = tableEnv.fromDataStream(sensorDStream);

        //3.注册函数
        tableEnv.registerFunction("Split",new MySplit());

        //4.TableAPI
        Table tableResult = table.joinLateral("split(id) as (word,length)")
                .select("id,word,length");

        //5.sql
        tableEnv.createTemporaryView("sensorid",table);
        Table sqlResult = tableEnv.sqlQuery("SELECT id, word, length FROM sensorid, LATERAL TABLE(split(id)) as T(word, length) ");


        //6.转化为流输出
        tableEnv.toRetractStream(tableResult, Row.class).print("tableResult");
        tableEnv.toRetractStream(sqlResult, Row.class).print("sqlResult");

        //7.执行
        env.execute();
    }
    public static class MySplit extends TableFunction<Tuple2<String,Integer>>{
        public void eval(String str){
            //对数据进行切分
            String[] split = str.split("_");

            for (String sp : split) {
                collect(new Tuple2<>(sp,sp.length()));
            }
        }
    }


}
