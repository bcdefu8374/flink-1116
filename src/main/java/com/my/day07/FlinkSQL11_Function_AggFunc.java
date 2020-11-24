package com.my.day07;

import com.my.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;


/**
 * @author chen
 * @topic
 * @create 2020-11-24
 */
public class FlinkSQL11_Function_AggFunc {
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

        //注册函数
        tableEnv.registerFunction("TempAvg",new TempAvg());


        //TableAPI
        Table tableResult = table.groupBy("id").select("id,tmp.TempAvg");

        //sql
        tableEnv.createTemporaryView("sensor",table);
        Table sqlResult = tableEnv.sqlQuery("select id,TempAvg(tmp) from sensor group by id");

        //转化为流输出
        tableEnv.toRetractStream(tableResult, Row.class).print("tableResult");
        tableEnv.toRetractStream(sqlResult, Row.class).print("sqlResult");

        //执行
        env.execute();
    }

    public static class TempAvg extends AggregateFunction<Double, Tuple2<Double,Integer>>{

        //初始化缓冲区
        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0D, 0);
        }

        //计算方法
        public void accumulate(org.apache.flink.api.java.tuple.Tuple2<Double, Integer> buffer, Double value) {
            buffer.f0 += value;
            buffer.f1 += 1;
        }

        //获取返回值结果
        @Override
        public Double getValue(Tuple2<Double, Integer> accumulator) {
            return accumulator.f0 / accumulator.f1;
        }
    }

}
