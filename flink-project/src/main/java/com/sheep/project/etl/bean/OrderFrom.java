package com.sheep.project.etl.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderFrom {
    private String dt;

    private String countryCode;

    private List<OrderData> orderData;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    static class OrderData {

        private String type;

        private String score;

        private String level;
    }

}
