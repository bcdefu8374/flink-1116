package com.my.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author chen
 * @topic
 * @create 2020-11-17
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class SensorReading {

    private String id;
    private Long ts;
    private Double tmp;



    @Override
    public String toString() {
        return id + ", " + ts + ", " + tmp;
    }

}
