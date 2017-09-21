package oracle.demo.oow.bd.hbase.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import oracle.demo.oow.bd.hbase.dao.MovieDAO;
import oracle.demo.oow.bd.hbase.util.HbaseDB;
import oracle.demo.oow.bd.to.ActivityTO;
import oracle.demo.oow.bd.to.CustomerTO;
import oracle.demo.oow.bd.to.GenreMovieTO;
import oracle.demo.oow.bd.to.GenreTO;
import oracle.demo.oow.bd.to.MovieTO;
import oracle.demo.oow.bd.pojo.RatingType;
import oracle.demo.oow.bd.util.StringUtil;

public class CustomerDAO {

	//修改user中的info和id列族
	public void insert(CustomerTO customerTO){
		
		HbaseDB db = HbaseDB.getInstance();
		Table table = db.getTable("user");
		if(table!=null){
			Put put1 =new Put(Bytes.toBytes(customerTO.getUserName()));
			//username-id的映射  索引
			put1.addColumn(Bytes.toBytes("id"), Bytes.toBytes("id"), Bytes.toBytes(customerTO.getId()));
			
			//用户基本信息
			Put put2 = new Put(Bytes.toBytes(customerTO.getId()));
			put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(customerTO.getName()));
			put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("username"), Bytes.toBytes(customerTO.getUserName()));
			put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("password"), Bytes.toBytes(customerTO.getPassword()));
			put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("email"), Bytes.toBytes(customerTO.getEmail()));
			
			List<Put> puts = new ArrayList<>();
			puts.add(put1);
			puts.add(put2);
			try {
				table.put(puts);
				table.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}

	public CustomerTO getCustomerByCredential(String username, String password) {
		// TODO Auto-generated method stub
		
		CustomerTO customerTO = null;
		
		//首先通过username查询id
		int id = getIdByUsername(username);
		//根据id查询基本信息
		if(id>0){
			customerTO = getInfoById(id);
			if(customerTO!=null){
				if(!customerTO.getPassword().equals(password)){
					customerTO = null;
				}
			}
		}
		return customerTO;
	}

	private CustomerTO getInfoById(int id) {
		// TODO Auto-generated method stub
		HbaseDB db = HbaseDB.getInstance();
		Table table = db.getTable("user");
		Get get = new Get(Bytes.toBytes(id));
		CustomerTO customerTO = new CustomerTO();
		try {
			Result result = table.get(get);
			customerTO.setEmail(Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("email"))));
			customerTO.setId(id);
			customerTO.setName(Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"))));
			customerTO.setPassword(Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("password"))));
			customerTO.setUserName(Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("username"))));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return customerTO;
	}

	private int getIdByUsername(String username) {
		// TODO Auto-generated method stub
		HbaseDB db = HbaseDB.getInstance();
		Table table = db.getTable("user");
		Get get = new Get(Bytes.toBytes(username));
		int id = 0;
		
		try {
			Result result = table.get(get);
			id = Bytes.toInt(result.getValue(Bytes.toBytes("id"), Bytes.toBytes("id")));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return id;
	}

	//获得分类信息
	public List<GenreMovieTO> getMovies4Customer(int custId,int movieMaxCount, int genreMaxCount){
		
		List<GenreMovieTO> genreMovies = new ArrayList<GenreMovieTO>();
		
		Filter filter = new PageFilter(genreMaxCount);
		Scan scan = new Scan();
		scan.setFilter(filter);
		scan.addFamily(Bytes.toBytes("genre"));
		
		HbaseDB db = HbaseDB.getInstance();
		Table table = db.getTable("genre");
		
		//全表扫描
		try {
			ResultScanner scanner = table.getScanner(scan);
			Iterator<Result> iter = scanner.iterator();
			GenreTO genreTO = null;
			
			while(iter.hasNext()){
				genreTO = new GenreTO();
				Result result = iter.next();
				genreTO.setId(Bytes.toInt(result.getRow()));
				genreTO.setName(Bytes.toString(result.getValue(Bytes.toBytes("genre"), Bytes.toBytes("name"))));
				GenreMovieTO gmovieTO = new GenreMovieTO();
				gmovieTO.setGenreTO(genreTO);
				genreMovies.add(gmovieTO);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}   
		return genreMovies;
	}
	
	//根据分类信息获取电影信息
	public List<MovieTO> getMovies4CustomerByGenre(int userId,int categoryId){
		
		List<MovieTO> movieTOs = new ArrayList<MovieTO>();
		
		HbaseDB db = HbaseDB.getInstance();
		Table table = db.getTable("genre");
		
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes("movie"));
		Filter filter = new PrefixFilter(Bytes.toBytes(categoryId+"_"));
		Filter filter2 = new PageFilter(25);
		FilterList filterList = new FilterList(filter,filter2);
		scan.setFilter(filterList);
		
		ResultScanner resultScanner;
		try {
			resultScanner = table.getScanner(scan);
			MovieTO movieTO = null;
			
			if(resultScanner!=null){
				Iterator<Result> iter = resultScanner.iterator();
				MovieDAO movieDao = new MovieDAO();
				while(iter.hasNext()){				
					Result result = iter.next();
					movieTO = movieDao.getMovieById(Bytes.toInt(result.getValue(Bytes.toBytes("movie"), Bytes.toBytes("movie_id"))));
					if(StringUtil.isEmpty(movieTO.getPosterPath())){
						movieTO.setOrder(100);
					}else{
						movieTO.setOrder(0);
					}
					movieTOs.add(movieTO);
				}
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return movieTOs;
	}
	
	public ActivityTO  getMovieRating(int userId,int movieId){
		
		ActivityTO activityTO = new ActivityTO();
		HbaseDB db = HbaseDB.getInstance();
		Table table = db.getTable("activity");
		Scan scan = new Scan();

		// 设置过滤器
		Filter filter = new SingleColumnValueFilter(Bytes.toBytes("activity"), Bytes.toBytes("user_id"),
				CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(userId)));
		((SingleColumnValueFilter) filter).setFilterIfMissing(true);
		
		Filter filter1 = new SingleColumnValueFilter(Bytes.toBytes("activity"), Bytes.toBytes("movie_id"),
				CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(movieId)));
		((SingleColumnValueFilter) filter).setFilterIfMissing(true);
		
		FilterList filterList = new FilterList(filter,filter1);
		scan.addFamily(Bytes.toBytes("activity"));
		scan.setFilter(filterList);
		try {
			ResultScanner resultScanner = table.getScanner(scan);
			if(resultScanner!=null){
				Iterator<Result> iter = resultScanner.iterator();
				while(iter.hasNext()){				
					Result result = iter.next();
					activityTO.setCustId(userId);
					activityTO.setMovieId(movieId);
					
					double price = 0.0;
					int position = 0;
					
					if(result.getValue(Bytes.toBytes("activity"), Bytes.toBytes("price"))==null)
						activityTO.setPrice(price);
					else
						activityTO.setPrice(Bytes.toDouble(result.getValue(Bytes.toBytes("activity"), Bytes.toBytes("price"))));
					
					if(result.getValue(Bytes.toBytes("activity"), Bytes.toBytes("position"))==null)
						activityTO.setPosition(position);
					else
						activityTO.setPosition(Bytes.toInt(result.getValue(Bytes.toBytes("activity"), Bytes.toBytes("position"))));
					
					activityTO.setRating(RatingType.getType(Bytes.toInt(result.getValue(Bytes.toBytes("activity"), Bytes.toBytes("rating")))));
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return activityTO;
	}
}
