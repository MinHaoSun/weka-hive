package com.huawei.bigdata.hive.example;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hive.jdbcDriver.MyHiveDriver;

import com.huawei.bigdata.security.LoginUtil;

public class HIVEJDBCExample {
	/**
	 * 所连接的集群是否为安全模式
	 */
	private static final boolean isSecureVerson = true;

	private static final String HIVE_DRIVER = "org.apache.hive.jdbc.MyHiveDriver";

	private static final String ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME = "Client";
	private static final String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";
	private static final String ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = "zookeeper/hadoop";
	private static final Properties INFO = null;

	private static Configuration CONF = null;
	private static String KRB5_FILE = null;
	private static String USER_NAME = null;
	private static String USER_KEYTAB_FILE = null;
	private static String zkQuorum = "*****zookeeper集群ip：port******";

	private static void init() throws IOException {
		CONF = new Configuration();
		// 设置新建用户的USER_NAME，其中"xxx"指代之前创建的用户名，例如创建的用户为user，则USER_NAME为user
		USER_NAME = "hdfs_user";

		if (isSecureVerson) {
			// 设置客户端的keytab和krb5文件路径
			USER_KEYTAB_FILE = "E:\\hdfs_user\\user.keytab";
			KRB5_FILE = "E:\\hdfs_user\\krb5.conf";
			LoginUtil.setJaasConf(ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME, USER_NAME, USER_KEYTAB_FILE);
			LoginUtil.setZookeeperServerPrincipal(ZOOKEEPER_SERVER_PRINCIPAL_KEY, ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL);

			// 安全模式
			// Zookeeper登录认证
			LoginUtil.login(USER_NAME, USER_KEYTAB_FILE, KRB5_FILE, CONF);
		}
	}

	/**
	 * 本示例演示了如何使用Hive JDBC接口来执行HQL命令<br>
	 * <br>
	 * 
	 * @throws ClassNotFoundException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 * @throws SQLException
	 * @throws IOException
	 */
	public static void main(String[] args)
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException, IOException {
		// 参数初始化
//		 init();

		// 加载Hive JDBC驱动
//		System.out.println(LoginUtil.checkNeedLogin(USER_NAME));
		// 定义HQL，HQL为单条语句，不能包含“;”
		String[] sqls = { "CREATE TABLE IF NOT EXISTS employees_info(id INT,name STRING)",
				"select * from table_201706252344040177567", "DROP TABLE employees_info" };

		// 拼接JDBC URL
		StringBuilder sBuilder = new StringBuilder("jdbc:hive2://").append(zkQuorum).append("/");

		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (isSecureVerson) {
			// 在使用多实例特性时append("hiveserver2;sasl.qop=auth-conf;auth=KERBEROS;principal=hive/hadoop.hadoop.com@HADOOP.COM")的"hiveserver2"与"hive/hadoop.hadoop.com@HADOOP.COM"根据使用不同的实例进行变更
			// 例如使用Hive1实例则改成"hiveserver2_1"与"hive1/hadoop.hadoop.com@HADOOP.COM"，Hive2实例为"hiveserver2_2",以此类推。
			sBuilder.append(";serviceDiscoveryMode=").append("zooKeeper").append(";zooKeeperNamespace=")
					.append("hiveserver2;sasl.qop=auth-conf;auth=KERBEROS;principal=hive/hadoop.hadoop.com@HADOOP.COM")
					.append(";");
		} else {
			// 普通模式
			// 使用多实例特性的"hiveserver2"变更参照安全模式
			sBuilder.append(";serviceDiscoveryMode=").append("zooKeeper").append(";zooKeeperNamespace=")
					.append("hiveserver2;auth=none");
		}
		String url = sBuilder.toString();
		System.out.println("url============" + url);
		Class.forName(HIVE_DRIVER);
		
		Connection connection = null;
		try {
			// 获取JDBC连接
			// 如果使用的是普通模式，那么第二个参数需要填写正确的用户名，否则会以匿名用户(anonymous)登录
			connection = DriverManager.getConnection(url, "", "");
			//connection = new MyHiveDriver().connect(url,INFO);

			// 建表
			// 表建完之后，如果要往表中导数据，可以使用LOAD语句将数据导入表中，比如从HDFS上将数据导入表:
			// load data inpath '/tmp/employees.txt' overwrite into table
			// employees_info;
			execDDL(connection, sqls[0]);
			System.out.println("Create table success!");

			// 查询
			execDML(connection, sqls[1]);

			// 删表
			execDDL(connection, sqls[2]);
			System.out.println("Delete table success!");
		} finally {
			// 关闭JDBC连接
			if (null != connection) {
				connection.close();
			}
		}
	}

	public static void execDDL(Connection connection, String sql) throws SQLException {
		PreparedStatement statement = null;
		try {
			statement = connection.prepareStatement(sql);
			statement.execute();
		} finally {
			if (null != statement) {
				statement.close();
			}
		}
	}

	private static void writeRow(List<Object> row, BufferedWriter csvWriter) throws IOException {
		// 写入文件头部
		for (Object data : row) {
			StringBuffer sb = new StringBuffer();
			String rowStr = sb.append("\"").append(data).append("\",").toString();
			csvWriter.write(rowStr);
		}
		csvWriter.newLine();
	}

	public static File createCSVFile(List<Object> head, List<List<Object>> dataList, String outPutPath,
			String filename) {
		File csvFile = null;
		BufferedWriter csvWtriter = null;
		try {
			csvFile = new File(outPutPath + File.separator + filename + ".csv");
			File parent = csvFile.getParentFile();
			if (parent != null && !parent.exists()) {
				parent.mkdirs();
			}
			csvFile.createNewFile();
			// GB2312使正确读取分隔符","
			csvWtriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(csvFile), "utf-8"), 1024);
			// 写入文件头部
			writeRow(head, csvWtriter);
			// 写入文件内容
			for (List<Object> row : dataList) {
				writeRow(row, csvWtriter);
			}
			csvWtriter.flush();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				csvWtriter.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return csvFile;
	}

	public static void execDML(Connection connection, String sql) throws SQLException {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		ResultSetMetaData resultMetaData = null;

		try {
			// 执行HQL
			statement = connection.prepareStatement(sql);
			resultSet = statement.executeQuery();

			// 输出查询的列名到控制台
			resultMetaData = resultSet.getMetaData();
			int columnCount = resultMetaData.getColumnCount();
			for (int i = 1; i <= columnCount; i++) {
				System.out.print(resultMetaData.getColumnLabel(i) + '\t');
			}
			System.out.println();

			// 输出查询结果到控制台
			while (resultSet.next()) {
				for (int i = 1; i <= columnCount; i++) {
					System.out.print(resultSet.getString(i) + '\t');
				}
				System.out.println();
			}
		} finally {
			if (null != resultSet) {
				resultSet.close();
			}

			if (null != statement) {
				statement.close();
			}
		}
	}

}
