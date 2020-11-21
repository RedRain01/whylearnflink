package model;

import lombok.Data;

/**
 * @author ：why
 * @description：TODO
 * @date ：2020/9/16 17:30
 */

@Data

public class Student {

    private String name;
    private String id;
    private String subject;
    private int score;
    private String clazz;
    private double tuition;
    private long time;
}
