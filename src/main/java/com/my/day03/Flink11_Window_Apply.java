package com.my.day03;

import com.my.day01.Flink01_wordCount_Batch;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * @author chen
 * @topic
 * @create 2020-11-18
 */
public class Flink11_Window_Apply {
    public static void main(String[] args) throws Exception {
        //1.获取创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.从端口获取数据
        DataStreamSource<String> word = env.socketTextStream("hadoop102", 7777);

        //3.对数据进行压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> tupleWindow = word.flatMap(new Flink01_wordCount_Batch.MyFlatMapper());

        //4.分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyByStream = tupleWindow.keyBy(0);

        //5.滚动时间开窗
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> timeWindow = keyByStream.timeWindow(Time.seconds(5));

        //6.计算
        SingleOutputStreamOperator<Integer> apply = timeWindow.apply(new MyWindowFun());

        //打印
        apply.print();

        //8.执行任务
        env.execute();
    }

    public static class MyWindowFun implements WindowFunction<Tuple2<String, Integer>,Integer,Tuple,TimeWindow>{

        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Integer> out) throws Exception {

            Integer count = 0;
            Iterator<Tuple2<String, Integer>> iterator = input.iterator();

            while (iterator.hasNext()){
                Tuple2<String, Integer> tuple2 = iterator.next();
                count += 1;
            }

            out.collect(count);
        }
    }
}
