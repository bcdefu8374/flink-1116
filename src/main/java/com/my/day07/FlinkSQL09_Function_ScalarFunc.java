package com.my.day07;

import com.my.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

/**
 * @author chen
 * @topic
 * @create 2020-11-24
 */
public class FlinkSQL09_Function_ScalarFunc {
    public static void main(String[] args) throws Exception {
        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //创建table执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.从端口读取数据转化为JavaBean
        DataStreamSource<String> textStream = env.socketTextStream("hadoop102", 7777);

        SingleOutputStreamOperator<SensorReading> sensorDStream = textStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {

                //对数据进行切分
                String[] split = value.split(",");
                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });

        //将流转化为表
        Table table = tableEnv.fromDataStream(sensorDStream);

        //注册函数
        tableEnv.registerFunction("Mylength",new Mylength());

        //TableAPI
        Table tableResult = table.select("id,id.Mylength");

        //sql
        tableEnv.createTemporaryView("sensor",table);
        Table sqlResult = tableEnv.sqlQuery("select id,Mylength(id) from sensor");

        //转化为流打印数据
        tableEnv.toAppendStream(tableResult, Row.class).print("tableResult");
        tableEnv.toAppendStream(sqlResult, Row.class).print("sqlResult");

        //执行
        env.execute();

    }

    public static class Mylength extends ScalarFunction{

        public int eval(String value){
            return value.length();
        }
    }

}
