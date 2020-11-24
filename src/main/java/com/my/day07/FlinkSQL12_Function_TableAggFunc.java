package com.my.day07;

import com.my.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;


/**
 * @author chen
 * @topic
 * @create 2020-11-24
 */
public class FlinkSQL12_Function_TableAggFunc {
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

        //2.注册函数
        tableEnv.registerFunction("TopNTmp",new TopNTmp());

        //3.TableAPI
        Table tableResult = table.groupBy("id").flatAggregate("TopNTmp(tmp) as (tmp,rank)").select("id,tmp,rank");


        //5.转化为流输出
        tableEnv.toRetractStream(tableResult, Row.class).print("tableResult");
        //6.执行
        env.execute();

    }

    public static class TopNTmp extends TableAggregateFunction<Tuple2<Double,Integer>,Tuple2<Double,Double>>{

        @Override
        public Tuple2<Double, Double> createAccumulator() {
            return new Tuple2<>(Double.MIN_VALUE,Double.MIN_VALUE);
        }

        public void accumulate(Tuple2<Double,Double> buffer, Double value){
            //1.将输入数据跟第一个比较
            if (value > buffer.f0){
                buffer.f1 = buffer.f0;
                buffer.f0 = value;
            }else if (value > buffer.f1){
                //将输入数据跟第二个比较
                buffer.f1 = value;
            }
        }

        public void emitValue(Tuple2<Double,Double> buffer, Collector<Tuple2<Double,Integer>> collector){
            collector.collect(new Tuple2<>(buffer.f0,1));
            if (buffer.f1 != Double.MAX_VALUE){
                collector.collect(new Tuple2<>(buffer.f1,2));
            }
        }
    }
}
