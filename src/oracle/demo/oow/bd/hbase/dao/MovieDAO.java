package oracle.demo.oow.bd.hbase.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import oracle.demo.oow.bd.hbase.util.HbaseDB;
import oracle.demo.oow.bd.to.CastCrewTO;
import oracle.demo.oow.bd.to.CastMovieTO;
import oracle.demo.oow.bd.to.CastTO;
import oracle.demo.oow.bd.to.CrewTO;
import oracle.demo.oow.bd.to.GenreTO;
import oracle.demo.oow.bd.to.MovieTO;
import oracle.demo.oow.bd.pojo.SearchCriteria;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;


public class MovieDAO{
   
	public void insert(MovieTO movieTO) {
		// TODO Auto-generated method stub
		HbaseDB db = HbaseDB.getInstance();
		Table table = db.getTable("movie");
		if(table!=null){
			//用户基本信息
			Put put1 = new Put(Bytes.toBytes(movieTO.getId()));
			put1.addColumn(Bytes.toBytes("movie"), Bytes.toBytes("original_title"), Bytes.toBytes(movieTO.getTitle()));
			put1.addColumn(Bytes.toBytes("movie"), Bytes.toBytes("release_date"), Bytes.toBytes(movieTO.getReleasedYear()));
			put1.addColumn(Bytes.toBytes("movie"), Bytes.toBytes("vote_count"), Bytes.toBytes(movieTO.getVoteCount()));
			put1.addColumn(Bytes.toBytes("movie"), Bytes.toBytes("popularity"), Bytes.toBytes(movieTO.getPopularity()));
			put1.addColumn(Bytes.toBytes("movie"), Bytes.toBytes("poster_path"), Bytes.toBytes(movieTO.getPosterPath()));
			put1.addColumn(Bytes.toBytes("movie"), Bytes.toBytes("runtime"), Bytes.toBytes(movieTO.getRunTime()));
			put1.addColumn(Bytes.toBytes("movie"), Bytes.toBytes("overview"), Bytes.toBytes(movieTO.getOverview()));
			
			//写入电影-分类映射
			List<GenreTO> genreTOs = movieTO.getGenres();
			Put put2 = null;
			GenreDAO genreDao = new GenreDAO();
		    for (GenreTO genreTO : genreTOs) {
		    	put2 = new Put(Bytes.toBytes(movieTO.getId()+"_"+genreTO.getId()));
				put2.addColumn(Bytes.toBytes("genre"), Bytes.toBytes("genre_id"), Bytes.toBytes(genreTO.getId()));
				put2.addColumn(Bytes.toBytes("genre"), Bytes.toBytes("genre_name"), Bytes.toBytes(genreTO.getName()));
			
				genreDao.insertGenre(genreTO);
				genreDao.insertGenresMovie(movieTO, genreTO);
		    }
			
			List<Put> puts = new ArrayList<>();
			puts.add(put1);
			puts.add(put2);
			
			try {
				table.put(puts);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public void insertMovieCast(CastTO castTO, int id) {
		// TODO Auto-generated method stub
		HbaseDB db = HbaseDB.getInstance();
		Table table = db.getTable("movie");
		
		Put put = new Put(Bytes.toBytes(id+"_"+castTO.getId()));
		put.addColumn(Bytes.toBytes("cast"), Bytes.toBytes("cast_id"), Bytes.toBytes(castTO.getId()));
		
		try {
			table.put(put);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void insertMovieCrew(CrewTO crewTO, Integer movieId) {
		// TODO Auto-generated method stub
		HbaseDB db = HbaseDB.getInstance();
		Table table = db.getTable("movie");
		
		Put put = new Put(Bytes.toBytes(movieId+"_"+crewTO.getId()));
		put.addColumn(Bytes.toBytes("crew"), Bytes.toBytes("crew_id"), Bytes.toBytes(crewTO.getId()));
		
		try {
			table.put(put);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public MovieTO getMovieById(int movieId) {
		// TODO Auto-generated method stub
		HbaseDB db = HbaseDB.getInstance();
		Table table = db.getTable("movie");
		Get get = new Get(Bytes.toBytes(movieId));
		MovieTO movieTO = new MovieTO();
		
		CrewDAO crewDao = new CrewDAO();
		CastDAO castDao = new CastDAO();
		CastCrewTO castCrewTO = new CastCrewTO();
		castCrewTO.setCrewList(crewDao.getMovieCrews(movieId));
		castCrewTO.setCastList(castDao.getMovieCasts(movieId));
		
		GenreDAO genreDao = new GenreDAO();
		
		try {
			Result result = table.get(get);
			movieTO.setId(movieId);
			movieTO.setGenres(genreDao.getMoviesByGenre(movieId));
			movieTO.setCastCrewTO(castCrewTO);
			movieTO.setTitle(Bytes.toString(result.getValue(Bytes.toBytes("movie"), Bytes.toBytes("original_title"))));
			movieTO.setOverview(Bytes.toString(result.getValue(Bytes.toBytes("movie"), Bytes.toBytes("overview"))));
			movieTO.setPosterPath(Bytes.toString(result.getValue(Bytes.toBytes("movie"), Bytes.toBytes("poster_path"))));
			int date = 0;
			if(result.getValue(Bytes.toBytes("movie"), Bytes.toBytes("release_date"))==null)
				date = 2017;
			else 
				date = Bytes.toInt(result.getValue(Bytes.toBytes("movie"), Bytes.toBytes("release_date")));
			movieTO.setDate(Integer.toString(date));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return movieTO;
	}
}
