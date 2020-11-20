package com.my.day04;

import com.my.day01.Flink01_wordCount_Batch;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author chen
 * @topic
 * @create 2020-11-20
 */
public class Flink09_State_WordCount {
    public static void main(String[] args) throws Exception {
        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.读取端口数据创建流
        DataStreamSource<String> socket = env.socketTextStream("hadoop102", 7777);

        //3.压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMap = socket.flatMap(new Flink01_wordCount_Batch.MyFlatMapper());

        //4.分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyBy = flatMap.keyBy(0);

        //5.使用状态编程方式实现workCount功能
        SingleOutputStreamOperator<Tuple2<String, Integer>> count = keyBy.map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {

            //定义状态
            private ValueState<Integer> countState = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                countState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("count", Integer.class, 0));
            }

            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                //获取状态信息
                Integer value1 = countState.value();
                //累计
                value1++;

                //更新状态
                countState.update(value1);

                //返回数据
                return new Tuple2<>(value.f0, value1);
            }
        });

        //6.打印
        count.print();

        //7.执行
        env.execute();

    }
}
