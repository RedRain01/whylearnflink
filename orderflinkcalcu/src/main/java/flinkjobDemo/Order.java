package flinkjobDemo;

import lombok.Data;

/**
 * @author ：why
 * @description：TODO
 * @date ：2020/10/24 14:27
 */

@Data
public class Order {
    private Integer cityId;
    private String userName;
    private String items;
    private String cityName;
    public Order(Integer cityId, String userName, String items, String cityName) {
        this.cityId = cityId;
        this.userName = userName;
        this.items = items;
        this.cityName = cityName;
    }
}
