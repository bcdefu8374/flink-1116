package com.my.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author chen
 * @topic
 * @create 2020-11-24
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WordCount {
    private String word;
    private Long count;




    @Override
    public String toString() {
        return word+ ", " + count;
    }
}
