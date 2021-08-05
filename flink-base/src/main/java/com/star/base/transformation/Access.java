package com.star.base.transformation;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author: zzStar
 * @Date: 08-05-2021 23:04
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Access {

    private Long time;

    private String domain;

    private Double traffic;

}
