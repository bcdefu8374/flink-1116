package com.my.day05;

import com.my.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author chen
 * @topic
 * @create 2020-11-21
 */
public class Flink01_State_tmp {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //指定使用事件时间,设置时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //读取端口数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 7777);

        //转化为javaBean
        SingleOutputStreamOperator<SensorReading> sensor = socketTextStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                //对数据进行切分
                String[] split = value.split(",");
                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });

        //2.分组
        KeyedStream<SensorReading, Tuple> sensorKeyBy = sensor.keyBy("id");

        //3.判断温度超过10度
        SingleOutputStreamOperator<Tuple3<String, Double, Double>> flatMap = sensorKeyBy.flatMap(new MyTempIncFun());

        //4.打印
        flatMap.print();
        
        //5.执行
        env.execute("Flink01_State_tmp");
    }

    public static class MyTempIncFun extends RichFlatMapFunction<SensorReading, Tuple3<String,Double,Double>>{


        //定义温度的状态上一次
        private ValueState<Double> lastTempState = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            //给温度做初始化
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("temp-last",Double.class));

        }

        @Override
        public void flatMap(SensorReading value, Collector<Tuple3<String, Double, Double>> out) throws Exception {
            //获取上一次温度值和当前状态温度值
            Double lasttemp = lastTempState.value();
            Double curtemp = value.getTmp();

            //判断跳变超过10
            if (lasttemp != null && Math.abs(lasttemp - curtemp) > 10.0){
                out.collect(new Tuple3<>(value.getId(),lasttemp,curtemp));
            }

            //更新状态
            lastTempState.update(curtemp);
        }
    }


}
