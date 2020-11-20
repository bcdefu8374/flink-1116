package com.my.day04;

import com.my.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author chen
 * @topic
 * @create 2020-11-20
 */
public class Flink06_ProcessAPI_KeyedProcessFunc {
    public static void main(String[] args) {
        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(1);
        //设置Event.time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //2.从端口获取数据
        DataStreamSource<String> sorketText = env.socketTextStream("hadoop102", 7777);

        //3.获取的数据转化为javaBean
        SingleOutputStreamOperator<SensorReading> wordBean = sorketText.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                //对数据进行切分
                String[] split = value.split(",");
                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });


        //4.分组
        KeyedStream<SensorReading, Tuple> keyById = wordBean.keyBy("id");

        //5.使用底层 的processAPI
        keyById.process(new MyKeyedProcess());
    }
    public static class MyKeyedProcess extends KeyedProcessFunction<Tuple,SensorReading,String>{

        //生命周期开始方法
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            //状态编程相关的
            RuntimeContext runtimeContext = getRuntimeContext();
            //ValueState<Object> state = runtimeContext.getState();

            //获取当前的key和时间戳
            Tuple currentKey = ctx.getCurrentKey();
            Long timestamp = ctx.timestamp();

            //定时服务相关的
            TimerService timerService = ctx.timerService();
            timerService.currentProcessingTime();
            timerService.registerProcessingTimeTimer(4);
            timerService.deleteProcessingTimeTimer(2);

            //侧输出流
            //ctx.output();

        }

        //生命周期结束方法
        @Override
        public void close() throws Exception {
            super.close();
        }
    }
}
