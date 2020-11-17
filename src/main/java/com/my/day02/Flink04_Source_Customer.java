package com.my.day02;

import com.my.bean.SensorReading;
import org.apache.commons.math3.fitting.leastsquares.EvaluationRmsChecker;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

/**
 * @author chen
 * @topic
 * @create 2020-11-17
 */
public class Flink04_Source_Customer {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.自定义kafka消费数据
        DataStreamSource<SensorReading> sensorReading = env.addSource(new CustomerSource());

        //3.打印
        sensorReading.print();

        //4.开启
        env.execute("Flink04_Source_Customer");
    }

    public static class CustomerSource implements SourceFunction<SensorReading>{

        //定义标志位控制数据接收
        private boolean bolg = true;

        Random random = new Random();
        @Override
        public void run(SourceContext ctx) throws Exception {
            //定义map
            HashMap<String, Double> map = new HashMap<>();

            //2.向map中添加基础数据
            for (int i = 0; i < 10; i++) {
                map.put("Sensor_" + i,+ 10 + random.nextGaussian() * 20);
            }

            //3.当标志位为true的时候
            while (bolg) {
                //遍历map
                for (String id : map.keySet()) {
                    //提取上一次传感器温度
                    Double temp = map.get(id);

                    double newTemp =  temp + random.nextGaussian();

                    ctx.collect(new SensorReading(id,System.currentTimeMillis(),newTemp));

                    //把当前数据再写回map
                    map.put(id,newTemp);

                }

                //每取出一个数据就睡一段时间
                Thread.sleep(2000);
            }



        }

        @Override
        public void cancel() {
            bolg = false;

        }
    }
}
