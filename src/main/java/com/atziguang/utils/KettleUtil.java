package com.atziguang.utils;

import com.alibaba.fastjson.JSONObject;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.kdr.KettleDatabaseRepository;
import org.pentaho.di.repository.kdr.KettleDatabaseRepositoryMeta;

import java.io.IOException;
import java.util.Properties;

/**
 * @author wuyong
 * @date 2021/04/22
 */
public class KettleUtil {
    public static JSONObject runWithDb(String jobName) throws KettleException, IOException {
        //获取kettle资源库基础配置
        Properties prop = PropertiesUtil.load("config.properties");
        String host = prop.getProperty("kettle.repo.host");
        String port = prop.getProperty("kettle.repo.port");
        String usr = prop.getProperty("kettle.repo.user");
        String pass = prop.getProperty("kettle.repo.pass");
        String kettle_user = prop.getProperty("kettle.user");
        String kettle_pass = prop.getProperty("kettle.password");
        //String[] arguments = {"1000"};
        KettleEnvironment.init();
        //创建DB资源库
        KettleDatabaseRepository repository = new KettleDatabaseRepository();
        DatabaseMeta databaseMeta = new DatabaseMeta("kettle", "postgresql", "jdbc", host, "kettle", port, usr, pass);
        //选择资源库
        KettleDatabaseRepositoryMeta kettleDatabaseRepositoryMeta = new KettleDatabaseRepositoryMeta("kettle", "kettle", "test", databaseMeta);
        repository.init(kettleDatabaseRepositoryMeta);
        //连接资源库
        repository.connect(kettle_user, kettle_pass);
        RepositoryDirectoryInterface directoryInterface = repository.loadRepositoryDirectoryTree();
        //选择job
        JobMeta jobMeta = repository.loadJob(jobName, directoryInterface, null, null);
        Job job = new Job(repository, jobMeta);
        //需要传参时
        //job.setArguments(arguments);
        job.setVariable("id","10000");
        job.execute(0, null);
        job.waitUntilFinished();
        JSONObject resultJson = new JSONObject();
        if (job.getErrors() > 0) {
            resultJson.put("result", "data trans false");
        } else {
            resultJson.put("result", "data trans success");
        }
        return resultJson;

    }
}
