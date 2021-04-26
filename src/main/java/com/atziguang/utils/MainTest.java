package com.atziguang.utils;

import com.alibaba.fastjson.JSONObject;
import org.pentaho.di.core.exception.KettleException;

import java.io.IOException;

public class MainTest {
    public static void main(String[] args) {
        //增量更新方式
        //String jobName = "dm_to_pg";
        //全量导入方式
        String jobName = "drop_insert";
        try {
            JSONObject resultJson = KettleUtil.runWithDb(jobName);
            Object result = resultJson.get("result");
            System.out.println(result);
        } catch (KettleException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
