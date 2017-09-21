package oracle.demo.oow.bd.hbase.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import oracle.demo.oow.bd.dao.BaseDAO;
import oracle.demo.oow.bd.hbase.util.HbaseDB;
import oracle.demo.oow.bd.to.GenreTO;
import oracle.demo.oow.bd.to.MovieTO;

import oracle.demo.oow.bd.pojo.SearchCriteria;
import oracle.demo.oow.bd.util.StringUtil;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class GenreDAO {

    private static final int TOP = 50;
    
    @SuppressWarnings("null")
	public List<GenreTO> getGenres(){
    	
    	HbaseDB db = HbaseDB.getInstance();
		Table table = db.getTable("genre");
    	GenreTO genreTO = null;
        List<GenreTO> genreList = new ArrayList<GenreTO>();
        
        ResultScanner resultScanner;
		try {
			resultScanner = table.getScanner(Bytes.toBytes("genre"));
			Iterator<Result> iter = resultScanner.iterator();
	   	    while(iter.hasNext()) {
	   	    	genreTO.setId(Bytes.toInt(iter.next().getRow()));
	   	    	genreTO.setName(Bytes.toString(iter.next().getValue(Bytes.toBytes("cast"), Bytes.toBytes("name"))));
	   	    	genreList.add(genreTO);
	   	    }
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        return genreList;
    }
    
    //插入genre基本信息（分类表）
    public void insertGenre(GenreTO genreTO) {
		// TODO Auto-generated method stub
    	
    	HbaseDB db = HbaseDB.getInstance();
		Table table = db.getTable("genre");
		if(table!=null){
			//用户基本信息
			Put put1 = new Put(Bytes.toBytes(genreTO.getId()));
			put1.addColumn(Bytes.toBytes("genre"), Bytes.toBytes("name"), Bytes.toBytes(genreTO.getName()));
			try {
				table.put(put1);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
    /**
     * This method insert movie to its different genre group, so that one can
     * query movies by genres. It also assign genres into the genre group so that
     * one can query all the unique genres that exist in the store at a point in
     * time.
     * @param movieTO
     */

    //插入genre-movie ID的映射
	public void insertGenresMovie(MovieTO movieTO, GenreTO genreTO) {
		// TODO Auto-generated method stub
		HbaseDB db = HbaseDB.getInstance();
		Table table = db.getTable("genre");
		if(table!=null){
			Put put = new Put(Bytes.toBytes(genreTO.getId()+"_"+movieTO.getId()));
			put.addColumn(Bytes.toBytes("movie"),Bytes.toBytes("movie_id"), Bytes.toBytes(movieTO.getId()));
			try {
				table.put(put);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
    /**
     * This method returns GenreTO for the genreName
     * @param genreName
     * @return GenreTO
     */
    public GenreTO getGenreById(int genreId) {
    	
        GenreTO genreTO = new GenreTO();
        HbaseDB db = HbaseDB.getInstance();
		Table table = db.getTable("genre");
		Get get = new Get(Bytes.toBytes(genreId));

		try {
			Result result = table.get(get);
			genreTO.setId(genreId);
			genreTO.setName(Bytes.toString(result.getValue(Bytes.toBytes("genre"), Bytes.toBytes("name"))));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        return genreTO;
    }


    public ArrayList<GenreTO> getMoviesByGenre(int movieId) {
    	
    	ArrayList<GenreTO> genres = new ArrayList<GenreTO>();
    	
    	HbaseDB db = HbaseDB.getInstance();
		Table table = db.getTable("movie");
		
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes("genre"));
		Filter filter = new PrefixFilter(Bytes.toBytes(movieId+"_"));
		scan.setFilter(filter);
		
		ResultScanner resultScanner = null;
		try {
			resultScanner = table.getScanner(scan);
			
			if(resultScanner!=null){
				Iterator<Result> iter = resultScanner.iterator();
				GenreTO genreTO = new GenreTO();
				while(iter.hasNext()){				
					Result result = iter.next();
					GenreDAO genreDao = new GenreDAO();
					int genreId = Bytes.toInt(result.getValue(Bytes.toBytes("genre"), Bytes.toBytes("genre_id")));
					genreTO = genreDao.getGenreById(genreId);
					genres.add(genreTO);
				}
			}
		}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        return genres;
    }

    /**
     * Overloaded method that returns top N movies for the genreID
     * @param genreId
     * @return
     */
    public List<MovieTO> getMoviesByGenre(int genreId, SearchCriteria sc) {
        return getMoviesByGenre(genreId, TOP, sc);
    }


    public List<MovieTO> getMoviesByGenre(int genreId, int movieCount) {
        return getMoviesByGenre(genreId, movieCount, null);
    }

    /**
     * This method takes searchCriteriaTO as an additional input to return
     * movies=movieCount that belong to genre=genreId
     * @param genreId
     * @param movieCount
     * @param sc
     * @return List of MovieTO
     */
    public List<MovieTO> getMoviesByGenre(int genreId, int movieCount, SearchCriteria sc) {
		return null;
    }

    @SuppressWarnings("unused")
	private boolean assertSearchCriteria(SearchCriteria sc, MovieTO movieTO) {
        boolean flag = true;
        if (sc != null && movieTO != null) {
            if (sc.isPosterPath() && StringUtil.isEmpty(movieTO.getPosterPath())) {
                flag = false;
            } else if (movieTO.getReleasedYear() < sc.getReleasedYear()) {
                flag = false;
            } else if (movieTO.getPopularity() < sc.getPopularity()) {
                flag = false;
            } else if (movieTO.getPopularity() < sc.getPopularity()) {
                flag = false;
            } else if (movieTO.getVoteCount() < sc.getVoteCount()) {
                flag = false;
            }
        } //EOF if

        return flag;
    }

}
