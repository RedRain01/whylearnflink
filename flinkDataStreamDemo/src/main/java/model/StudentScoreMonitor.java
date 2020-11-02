package model;

import lombok.Data;

/**
 * @author ：why
 * @description：TODO
 * @date ：2020/9/28 0:16
 */

@Data
public class StudentScoreMonitor {
    private String name;
    private int score;
    private long time;
    private long lastModified;
}
