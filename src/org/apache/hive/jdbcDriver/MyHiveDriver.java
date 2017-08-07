package org.apache.hive.jdbcDriver;

import com.huawei.bigdata.security.LoginUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hive.jdbc.HiveDriver;

public class MyHiveDriver
  extends HiveDriver
{
  static
  {
    String ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME = "Client";
    String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";
    String ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = "zookeeper/hadoop";
    
    Configuration CONF = new Configuration();
    
    String zkQuorum = "*****zookeeper集群ip：port******";
    
    String USER_NAME = "hdfs_user";
    
    String USER_KEYTAB_FILE = "E:\\hdfs_user\\user.keytab";
    String KRB5_FILE = "E:\\hdfs_user\\krb5.conf";
    try
    {
      LoginUtil.setJaasConf("Client", USER_NAME, USER_KEYTAB_FILE);
      LoginUtil.setZookeeperServerPrincipal("zookeeper.server.principal", "zookeeper/hadoop");
      
      LoginUtil.login(USER_NAME, USER_KEYTAB_FILE, KRB5_FILE, CONF);
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }
  
  public static void main(String[] args) {}
}
