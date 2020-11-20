package com.my.day03;

import com.my.day01.Flink01_wordCount_Batch;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @author chen
 * @topic
 * @create 2020-11-18
 */
public class Flink07_Window_Offset {
    public static void main(String[] args) throws Exception {
        //1.获取创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.从端口获取数据
        DataStreamSource<String> word = env.socketTextStream("hadoop102", 7777);

        //3.对数据进行压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> tupleWindow = word.flatMap(new Flink01_wordCount_Batch.MyFlatMapper());

        //4.分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyByStream = tupleWindow.keyBy(0);

        //5.开窗
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> window = keyByStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5)));

        //6.聚合
        window.sum(1).print();

        //7.执行
        env.execute("Flink07_Window_Offset");
    }
}
