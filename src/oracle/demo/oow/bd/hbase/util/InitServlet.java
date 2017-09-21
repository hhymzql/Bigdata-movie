package oracle.demo.oow.bd.hbase.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Properties;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import oracle.demo.oow.bd.util.FileWriterUtil;

@SuppressWarnings("serial")
public class InitServlet extends HttpServlet {

	public void init(ServletConfig config) throws ServletException{
		//读取文件
		super.init(config);
		String path;
		FileInputStream fis;
		try{
			path = InitServlet.class.getResource("/").getPath();
			fis = new FileInputStream(path+"conf.properties");
			Properties properties = new Properties();
			properties.load(fis);
			fis.close();
			String outputFile = properties.getProperty("output_file");
			if(outputFile!=null){
				FileWriterUtil.OUTPUT_FILE = outputFile;
			}
			
			String zookeeper = properties.getProperty("hbase.zookeeper.quorum");
			if(zookeeper!=null){
				ConstantsHBase.ZOOKEEPER = zookeeper;
			}
			String hbaseRootDir = properties.getProperty("hbase.rootdir");
			if(hbaseRootDir!=null){
				ConstantsHBase.HBASE_ROOT_DIR = hbaseRootDir;
			}
			String mysqlusername = properties.getProperty("mysql.username");
			if(hbaseRootDir!=null){
				ConstantsHBase.MYSQL_USERNAME = mysqlusername;
			}
			String mysqlPassword = properties.getProperty("mysql.password");
			if(hbaseRootDir!=null){
				ConstantsHBase.MYSQL_PASSWORD = mysqlPassword;
			}
			String mysqlDriver = properties.getProperty("mysql.driver");
			if(hbaseRootDir!=null){
				ConstantsHBase.MYSQL_DRIVER = mysqlDriver;
			}
			String mysqlURL = properties.getProperty("mysql.url");
			if(hbaseRootDir!=null){
				ConstantsHBase.MYSQL_URL = mysqlURL;
			}
		}catch (Exception e) {
			e.printStackTrace();
		}
		
		
	}

}
