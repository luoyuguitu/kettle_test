package com.atziguang.utils;

import java.io.IOException;
import java.util.Properties;

public class PropertiesUtil {
    public static Properties load(String propertiesName) throws IOException {
        Properties prop = new Properties();
        prop.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(propertiesName));
        return prop;
    }
}
