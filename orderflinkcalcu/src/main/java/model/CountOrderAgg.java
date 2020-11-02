package model;

import lombok.Data;

import java.sql.Timestamp;

/**
 * @author ：why
 * @description：TODO
 * @date ：2020/10/31 17:06
 */

@Data
public class CountOrderAgg  {
    private String subject;
    private String name;
    private String orderTime;
    private long count;
    private long startTime;
    private long endTime;
    private Double countAmount;

}
