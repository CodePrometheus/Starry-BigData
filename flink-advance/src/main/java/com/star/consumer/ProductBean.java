package com.star.consumer;

import lombok.*;
import lombok.experimental.Accessors;

import java.io.Serializable;
import java.math.BigDecimal;

@Data
@Builder
@ToString
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
public class ProductBean implements Serializable {
    private String productName;
    private Integer status;
    private BigDecimal price;
}
