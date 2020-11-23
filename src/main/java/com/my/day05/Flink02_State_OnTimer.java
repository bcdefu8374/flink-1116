package com.my.day05;

import com.my.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author chen
 * @topic
 * @create 2020-11-21
 */
public class Flink02_State_OnTimer {
    public static void main(String[] args) throws Exception {
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

        //3.判断温度10s没有下降,要同时用到状态编程所以用底层的 processAPI
        sensorKeyBy.process(new MykedByProcessFun());

        //打印

        //执行
        env.execute();
    }

    public static class MykedByProcessFun extends KeyedProcessFunction<Tuple,SensorReading,String>{

        //定义温度的状态上一次
        private ValueState<Double> lastTempState = null;
        //对此次和下次的闹钟时间做状态
        private ValueState<Long> lastTsState = null;


        @Override
        public void open(Configuration parameters) throws Exception {
            //给温度做初始化
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("temp-last",Double.class));
            //对闹钟时间做初始化
            lastTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-last",Long.class));
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            //提取上一次的温度值
            Double lastTemp = lastTempState.value();
            //提取上一次的定时器时间
            Long lastTs = lastTsState.value();
            //设置当前时间闹钟
            long ts = ctx.timerService().currentProcessingTime() + 5000L;

            //第一条数据,需要注册定时器闹钟
            if(lastTs == null){
                //注册定时器
                ctx.timerService().registerProcessingTimeTimer(ts);
                lastTsState.update(ts);
            }else {
                if (lastTemp != null && value.getTmp() < lastTemp) {
                    //删除定时器
                    ctx.timerService().deleteProcessingTimeTimer(lastTsState.value());
                    //重新注册定时器
                    ctx.timerService().registerProcessingTimeTimer(ts);
                    //更新定时器
                    lastTsState.update(ts);
                }
            }
            //更新状态
            lastTempState.update(value.getTmp());

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect(ctx.getCurrentKey() + "连续10秒温度没有下降");
            lastTsState.clear();
        }
    }
}
