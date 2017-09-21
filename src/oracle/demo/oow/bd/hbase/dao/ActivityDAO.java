package oracle.demo.oow.bd.hbase.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import oracle.demo.oow.bd.hbase.util.HbaseDB;
import oracle.demo.oow.bd.hbase.util.ConstantsHBase;
import oracle.demo.oow.bd.pojo.ActivityType;
import oracle.demo.oow.bd.pojo.RatingType;
import oracle.demo.oow.bd.to.ActivityTO;
import oracle.demo.oow.bd.to.CastCrewTO;
import oracle.demo.oow.bd.to.MovieTO;
import oracle.demo.oow.bd.util.FileWriterUtil;
import oracle.demo.oow.bd.util.KeyUtil;
import oracle.kv.table.PrimaryKey;

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
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class ActivityDAO {

    public void insertCustomerActivity(ActivityTO activityTO) {
        int custId = 0;
        int movieId = 0;
        ActivityType activityType = null;
        String jsonTxt = null;

        System.out.println("Adao:activityTO="+activityTO);
        if (activityTO != null) {
            jsonTxt = activityTO.getJsonTxt();
            System.out.println("User Activity| " + jsonTxt);
            /**
             * This system out should write the content to the application log
             * file.
             */
            FileWriterUtil.writeOnFile(activityTO.getActivityJsonOriginal().toString());
            
            custId = activityTO.getCustId();
            movieId = activityTO.getMovieId();

            if (custId > 0 && movieId > 0) {
                activityType = activityTO.getActivity();

                HbaseDB db = HbaseDB.getInstance();
                Long id = db.getId(ConstantsHBase.TABLE_GID,ConstantsHBase.FAMILY_GID_GID,ConstantsHBase.QUALIFIER_GID_ACTIVITY_ID);
                
                Table table = db.getTable(ConstantsHBase.TABLE_ACTIVITY);
                Put put = new Put(Bytes.toBytes(id));
                put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_MOVIE_ID), Bytes.toBytes(activityTO.getMovieId()));
                put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_ACTIVITY), Bytes.toBytes(activityTO.getActivity().getValue()));
                put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_GENRE_ID), Bytes.toBytes(activityTO.getGenreId()));
                put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_POSITION), Bytes.toBytes(activityTO.getPosition()));
                put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_PRICE), Bytes.toBytes(activityTO.getPrice()));
                put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_RATING), Bytes.toBytes(activityTO.getRating().getValue()));
                put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_RECOMMENDED), Bytes.toBytes(activityTO.isRecommended().getValue()));
                put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_TIME), Bytes.toBytes(activityTO.getTimeStamp()));
                put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_USER_ID), Bytes.toBytes(activityTO.getCustId()));                
                
                try {
					table.put(put);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
            } //if (custId > 0 && movieId > 0)

        } //if (activityTO != null)

    } //insetCustomerActivity
    
    public List<MovieTO> getCustomerCurrentWatchList(int userId){
    	
    	List<MovieTO> movies = new ArrayList<MovieTO>();
    	MovieTO movieTO = new MovieTO();
    	
    	HbaseDB db = HbaseDB.getInstance();
    	Table table = db.getTable("activity");
    	Scan scan = new Scan();
    	scan.addFamily(Bytes.toBytes("activity"));
    	
    	Filter filter = new SingleColumnValueFilter(Bytes.toBytes("activity"), Bytes.toBytes("user_id"),
    			CompareFilter.CompareOp.EQUAL,new BinaryComparator(Bytes.toBytes(userId)));
    	scan.setFilter(filter);
    	
    	try {
			ResultScanner scanner = table.getScanner(scan);
			if(scanner!=null){
				Iterator<Result> iter = scanner.iterator();
				while(iter.hasNext()){	
					Result result = iter.next();
					int id = Bytes.toInt(result.getValue(Bytes.toBytes("activity"), Bytes.toBytes("movie_id")));
					movieTO = new MovieDAO().getMovieById(id);
					movies.add(movieTO);
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
		return movies;
    	
    }
    
    public ActivityTO getActivityTO(int userId, int movieId){
    	
    	ActivityTO activityTO = new ActivityTO();
    	CustomerDAO customerDao = new CustomerDAO();
    	activityTO = customerDao.getMovieRating(userId, movieId);
		return activityTO;
    }
    
    public List<MovieTO> getCommonPlayList(){
		
    	int activity = 5;
    	List<MovieTO> movies = new ArrayList<MovieTO>();
    	MovieTO movieTO = new MovieTO();
    	
    	HbaseDB db = HbaseDB.getInstance();
    	Table table = db.getTable("activity");
    	Scan scan = new Scan();
    	scan.addFamily(Bytes.toBytes("activity"));
    	
    	Filter filter = new SingleColumnValueFilter(Bytes.toBytes("activity"), Bytes.toBytes("activity"),
    			CompareFilter.CompareOp.EQUAL,new BinaryComparator(Bytes.toBytes(activity)));
    	scan.setFilter(filter);
    	
    	try {
			ResultScanner scanner = table.getScanner(scan);
			if(scanner!=null){
				Iterator<Result> iter = scanner.iterator();
				while(iter.hasNext()){	
					Result result = iter.next();
					int id = Bytes.toInt(result.getValue(Bytes.toBytes("activity"), Bytes.toBytes("movie_id")));
					movieTO = new MovieDAO().getMovieById(id);
					movies.add(movieTO);
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
		return movies;  	
    }
    
    public List<MovieTO> getCustomerHistoricWatchList(int userId){
    	
    	List<MovieTO> movies = new ArrayList<MovieTO>();
    	movies = new ActivityDAO().getCustomerCurrentWatchList(userId);
		return movies;
    }
    
    public List<MovieTO> getCustomerBrowseList(int userId){
    	
    	List<MovieTO> movies = new ArrayList<MovieTO>();
    	MovieTO movieTO = new MovieTO();
    	
    	HbaseDB db = HbaseDB.getInstance();
    	Table table = db.getTable("activity");
    	Scan scan = new Scan();
    	
    	Filter filter = new SingleColumnValueFilter(Bytes.toBytes("activity"), Bytes.toBytes("user_id"), 
    			CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(userId)));
    	
    	Filter filter1 = new SingleColumnValueFilter(Bytes.toBytes("activity"), Bytes.toBytes("activity"), 
    			CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(ActivityType.BROWSED_MOVIE.getValue())));
    	
    	FilterList filterList = new FilterList(filter,filter1);
    	scan.addFamily(Bytes.toBytes("activity"));
    	scan.setFilter(filterList);
    	
    	try {
			ResultScanner scanner = table.getScanner(scan);
			if(scanner!=null){
				Iterator<Result> iter = scanner.iterator();
				while(iter.hasNext()){
					Result result = iter.next();
					int movieId = Bytes.toInt(result.getValue(Bytes.toBytes("activity"), Bytes.toBytes("movie_id")));
					movieTO = new MovieDAO().getMovieById(movieId);
					movies.add(movieTO);
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
		return movies;
    }
}