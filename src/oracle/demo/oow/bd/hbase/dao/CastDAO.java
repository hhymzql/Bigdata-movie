package oracle.demo.oow.bd.hbase.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import oracle.demo.oow.bd.hbase.util.ConstantsHBase;
import oracle.demo.oow.bd.hbase.util.HbaseDB;
import oracle.demo.oow.bd.to.CastCrewTO;
import oracle.demo.oow.bd.to.CastMovieTO;
import oracle.demo.oow.bd.to.CastTO;
import oracle.demo.oow.bd.to.CrewTO;
import oracle.demo.oow.bd.to.MovieTO;

public class CastDAO{

    /**
     * This method inserts Cast information and also index cast ID to the movieId
     * so that one can run queries on castId to fetch all the movies in which
     * a particular cast worked.
     * @param castTO - This is a cast transfer object
     * @return - true if insertion is successful
     */
    public boolean insertCastInfo(CastTO castTO) {
        boolean flag = false;
        return flag;
    } //insertCastInfo

    /**
     * This method loops through all the movieId stored in the CastTO and create
     * association between movieId and castId and saves CastCrewTO JSON txt as the
     * value. Key=/MV/movieId/-/cc
     * @param castTO
     */
    public void insertCast4Movies(CastTO castTO) {
        List<CastMovieTO> castMovieList = null;
        CastCrewTO castCrewTO = null;

    } //insertCast4Movies

    /**
     * This method returns CastTO when castId is passed
     * @param castId uniue Cast Id
     * @return CastTO
     */
    public CastTO getCastById(int movieId,int castId) {
        
        HbaseDB db = HbaseDB.getInstance();
		Table table = db.getTable("cast");
		
		CastTO castTO = new CastTO();
		Get get = new Get(Bytes.toBytes(castId));
		
		Filter filter = new PrefixFilter(Bytes.toBytes(castId+"_"));
		Scan scan = new Scan();
		scan.setFilter(filter);
		scan.addFamily(Bytes.toBytes("cast"));

		try {
			Result result = table.get(get);
			castTO.setId(castId);
			castTO.setName(Bytes.toString(result.getValue(Bytes.toBytes("cast"), Bytes.toBytes("name"))));
			
			ResultScanner resultScanner = table.getScanner(scan);
			if(resultScanner!=null){
				Iterator<Result> iter = resultScanner.iterator();
				while(iter.hasNext()){
					Result result1 = iter.next();
					int order = Bytes.toInt(result1.getValue(Bytes.toBytes("cast"), Bytes.toBytes("order")));
					String character = Bytes.toString(result1.getValue(Bytes.toBytes("cast"), Bytes.toBytes("character")));
					castTO.setOrder(order);
					castTO.setCharacter(character);
				}
			}
		}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        return castTO;
    } //getCastById

    /**
     * This method returns a list of Casts for the movieId passed
     * Key= /MV/movieId/-/cc
     * @param movieId - Unique Id of the movie
     * @return List of CastTO
     */
	public List<CastTO> getMovieCasts(int movieId) {

        List<CastTO> castList = new ArrayList<CastTO>();

        HbaseDB db = HbaseDB.getInstance();
		Table table = db.getTable("movie");
		
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes("cast"));
		Filter filter = new PrefixFilter(Bytes.toBytes(movieId+"_"));
		scan.setFilter(filter);

		ResultScanner resultScanner = null;
		try {
			resultScanner = table.getScanner(scan);
			if(resultScanner!=null){
				Iterator<Result> iter = resultScanner.iterator();
				CastDAO castDao = new CastDAO();
				CastTO castTO = new CastTO();
				while(iter.hasNext()){				
					Result result = iter.next();
					castTO = castDao.getCastById(movieId,Bytes.toInt(result.getValue(Bytes.toBytes("cast"), Bytes.toBytes("cast_id"))));
					castList.add(castTO);
				}
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        return castList;
    } //getMovieCasts

    /**
     * This method returns all the movies that Cast worked in.
     * @param castId
     * @return List of MovieTO
     */
    public List<MovieTO> getMoviesByCast(int castId) {
    	List<MovieTO> movieList = new ArrayList<MovieTO>();
    	Scan scan = new Scan();
    	MovieTO movieTO = new MovieTO();
    	HbaseDB db = HbaseDB.getInstance();
    	Table table = db.getTable("cast");
    	scan.addFamily(Bytes.toBytes("movie"));
    	Filter filter = new PrefixFilter(Bytes.toBytes(castId+"_"));
    	scan.setFilter(filter);
    	MovieDAO movieDAO = new MovieDAO();
    	ResultScanner resultScanner;
    	try {
    		resultScanner = table.getScanner(scan);
    		if(resultScanner!=null){
    			Iterator<Result> iter = resultScanner.iterator();
    			while(iter.hasNext())
    			{
	    			Result result = iter.next();
	    			movieTO =movieDAO.getMovieById(Bytes.toInt(result.getValue(Bytes.toBytes("movie"), Bytes.toBytes("movie_id"))));
	    			movieList.add(movieTO);
    			}   
    		}
    	}
    	catch (IOException e) {
    		e.printStackTrace();
    	}
    	return movieList;
    }
}
