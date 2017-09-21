package oracle.demo.oow.bd.hbase.util;

import java.io.IOException;

import oracle.demo.oow.bd.hbase.util.ConstantsHBase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseDB {

	private Connection conn;
	private static class DBInstance{
		public static final HbaseDB instance = new HbaseDB();
	}
	
	public static HbaseDB getInstance(){
		return DBInstance.instance;
	}
	private HbaseDB(){
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop");
		conf.set("hbase.rootdir", "hdfs://hadoop:9000/hbase");
		try {
			conn = ConnectionFactory.createConnection(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	//根据表名称和列族创建表
	public void createTable(String tableName, String[] columnFamilies ,int versions) {
		// TODO Auto-generated method stub
		try {
			Admin admin = conn.getAdmin();
			//指定表名称
			HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
		
			//添加列族
			for (String string : columnFamilies) {
				HColumnDescriptor family = new HColumnDescriptor(Bytes.toBytes(string));
				//设置版本号，默认为1
				family.setMaxVersions(versions);
				descriptor.addFamily(family);
			}
			admin.createTable(descriptor);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	//根据表名称删除表
	public void deleteTable(String tableName){
		try {
			Admin admin = conn.getAdmin();
			if(admin.tableExists(TableName.valueOf(tableName))){
				//首先disable
				admin.disableTable(TableName.valueOf(tableName));
				//drop
				admin.deleteTable(TableName.valueOf(tableName));
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	//根据表名称获取Table对象
	public Table getTable(String tableName) {
		try {
			return conn.getTable(TableName.valueOf(tableName));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	//根据计数器计算行键
	public Long getId(String tableName, String family,
			String qualifier) {
		// TODO Auto-generated method stub
		Table table = getTable(tableName);
		
		try {
			return table.incrementColumnValue(Bytes.toBytes(ConstantsHBase.ROW_KEY_GID_ACTIVITY_ID),
					Bytes.toBytes(family), Bytes.toBytes(qualifier), 1);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0l;
	}
	
	//关闭连接
	public void close(){
		try {
			conn.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	 
}
