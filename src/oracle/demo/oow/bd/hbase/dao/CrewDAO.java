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
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import oracle.demo.oow.bd.hbase.util.HbaseDB;
import oracle.demo.oow.bd.to.CastCrewTO;
import oracle.demo.oow.bd.to.CrewTO;
import oracle.demo.oow.bd.to.MovieTO;
import oracle.demo.oow.bd.util.StringUtil;

public class CrewDAO{

    /**
     * This method inserts Crew information and also index crew ID to the movieId
     * so that one can run queries on castId to fetch all the movies in which
     * a particular cast worked.
     * @param crewTO - This is a cast transfer object
     * @return - true if insertion is successful
     */
    public boolean insertCrewInfo(CrewTO crewTO) {
        boolean flag = false;
        String jsonTxt = null;
        int crewId;
        String value = null;
        

        if (crewTO != null) {

            jsonTxt = crewTO.getJsonTxt();
            crewId = crewTO.getId();

            insertCrew4Movies(crewTO);

        }
        return flag;
    } //insertCastInfo


    /**
     * This method loops through all the movieId stored in the CastTO and create
     * association between movieId and castId and saves CastTO's JSON txt as the
     * value. Key= /MV_CW/movieId/-/crewId
     * @param crewTO
     */
    public void insertCrew4Movies(CrewTO crewTO) {
        List<String> movieList = null;
        CastCrewTO castCrewTO = null;
        int movieId = 0;
        int crewId = 0;
        String jsonTxt = null;

        if (crewTO != null & crewTO.getMovieList() != null) {

            movieList = crewTO.getMovieList();
            crewId = crewTO.getId();

            //We don't need cast movie list so make it null
            crewTO.setMovieList(null);

            for (String movieIdStr : movieList) {
                movieId = Integer.parseInt(movieIdStr);


                if (StringUtil.isNotEmpty(jsonTxt)) {
                    castCrewTO = new CastCrewTO(jsonTxt.trim());

                } else {
                    castCrewTO = new CastCrewTO();
                    castCrewTO.setMovieId(movieId);
                }

                //System.out.println("CastCrewInfo: " + castCrewTO.getJsonTxt());
                //add CastTO into castCrewTO
                castCrewTO.addCrewTO(crewTO);

                //Convert castCrewTO into json txt
                jsonTxt = castCrewTO.getJsonTxt();

                System.out.println("JSON = " +jsonTxt);

                //inset movie-cast association into the KV-Store
            } //if (order > -1 && order < 15) {
        } //EOF if (crewTO != null & crewTO.getMovieList() != null)

    } //insertCrew4Movies


    /**
     * This method returns CrewTO when crewId is passed
     * @param crewId uniue Cast Id
     * @return CrewTO
     */
	public CrewTO getCrewById(int movieId,int crewId) {
        CrewTO crewTO = new CrewTO();
        HbaseDB db = HbaseDB.getInstance();
		Table table = db.getTable("crew");
		Get get = new Get(Bytes.toBytes(crewId));

		try {
			Result result = table.get(get);
			crewTO.setId(crewId);
			crewTO.setMovieId(movieId);
			crewTO.setName(Bytes.toString(result.getValue(Bytes.toBytes("crew"), Bytes.toBytes("name"))));
			crewTO.setJob(Bytes.toString(result.getValue(Bytes.toBytes("crew"), Bytes.toBytes("job"))));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        return crewTO;
    } //getCastById

    /**
     * This method returns a list of Casts for the movieId passed
     * Key= /MV_CW/movieId/-/crewId
     * @param movieId - Unique Id of the movie
     * @return List of CrewTO
     */
	public List<CrewTO> getMovieCrews(int movieId) {

        List<CrewTO> crewList = new ArrayList<CrewTO>();

        HbaseDB db = HbaseDB.getInstance();
		Table table = db.getTable("movie");
		
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes("crew"));
		Filter filter = new PrefixFilter(Bytes.toBytes(movieId+"_"));
		scan.setFilter(filter);

		ResultScanner resultScanner = null;
		try {
			resultScanner = table.getScanner(scan);
			
			if(resultScanner!=null){
				Iterator<Result> iter = resultScanner.iterator();
				CrewTO crewTO = new CrewTO();
				while(iter.hasNext()){				
					Result result = iter.next();
					CrewDAO crewDao = new CrewDAO();
					crewTO = crewDao.getCrewById(movieId,Bytes.toInt(result.getValue(Bytes.toBytes("crew"), Bytes.toBytes("crew_id"))));
					crewList.add(crewTO);
				}
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        return crewList;
    } //getMovieCrews

    /**
     * This method returns all the movies that Crew worked in.
     * @param crewId
     * @return List of MovieTO
     */
    public List<MovieTO> getMoviesByCrew(int crewId) {
        List<String> movieIdList = null;
        List<MovieTO> movieList = new ArrayList<MovieTO>();
        CrewTO crewTO = null;
        MovieTO movieTO = null;
        MovieDAO movieDAO = new MovieDAO();

        String value = null;
        
        //System.out.println("--> : " + jsonTxt);
        return movieList;

    } //getMoviesByCrew
}//CrewDAO
