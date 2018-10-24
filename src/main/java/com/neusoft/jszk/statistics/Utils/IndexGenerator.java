package com.neusoft.jszk.statistics.Utils;

import java.util.UUID;

/**
 * 类说明：索引名称创建工具类
 */
public class IndexGenerator {
    public static String generate(){
        return UUID.randomUUID().toString().toUpperCase().replaceAll("-", "");
    }
}
