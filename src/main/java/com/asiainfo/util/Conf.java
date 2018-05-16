/**
 * Created by migle on 2016/8/10.
 */
package com.asiainfo.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Conf {
    private static final Properties param = new Properties();

    static {
        InputStream in = Conf.class.getClassLoader().getResourceAsStream("ai-event.properties");
        try {
            param.load(in);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
    public static final String kafka = param.getProperty("kafka.broker");
    public static final String groupid = param.getProperty("kafka.groupid");
    public static final String topic = param.getProperty("kafka.topic");
    public static final String zk = param.getProperty("zk.hosts");
}
