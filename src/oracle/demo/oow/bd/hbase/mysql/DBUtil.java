package oracle.demo.oow.bd.hbase.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import oracle.demo.oow.bd.hbase.util.ConstantsHBase;
public class DBUtil {
	private static String driver = ConstantsHBase.MYSQL_DRIVER;
	private static String URL = ConstantsHBase.MYSQL_URL;
	private static String dbusername = ConstantsHBase.MYSQL_USERNAME;
	private static String dbpassword = ConstantsHBase.MYSQL_PASSWORD;
	private Connection conn = null;
	private Statement stmt = null;

	public Connection getConn() {
		Connection dbConn = null;
		try
		{
		Class.forName(driver);
		System.out.println("连接驱动成功");
		}catch(Exception e){
		e.printStackTrace();
		System.out.println("连接驱动失败");
		}
		try{
	    dbConn =DriverManager.getConnection(URL,dbusername,dbpassword);
		System.out.println("数据库连接成功");
		stmt = dbConn.createStatement();
		}catch(Exception e)
		{
		e.printStackTrace();
		System.out.print("数据库连接失败");
		} 
		return dbConn;
	}

	public int executeUpdate(String s) {
		int result = 0;
		try {
			result = stmt.executeUpdate(s);
		} catch (Exception ex) {
			System.out.println("执行更新错误");
			ex.printStackTrace();
		}
		return result;
	}

	public ResultSet executeQuery(String s) {
		ResultSet rs = null;
		try {
			rs = stmt.executeQuery(s);
			System.out.println("执行查询成功");
		} catch (Exception ex) {
			System.out.println("执行查询失败");
		}
		return rs;
	}

	public void close() {
		try {
			stmt.close();
			conn.close();
		} catch (Exception e) {
		}
	}
}
