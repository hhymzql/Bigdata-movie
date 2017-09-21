
package oracle.demo.oow.bd.hbase.loder;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;

import java.io.IOException;

import oracle.demo.oow.bd.constant.Constant;
import oracle.demo.oow.bd.hbase.dao.CastDAO;
import oracle.demo.oow.bd.hbase.dao.CrewDAO;
import oracle.demo.oow.bd.hbase.dao.MovieDAO;
import oracle.demo.oow.bd.to.CastTO;
import oracle.demo.oow.bd.to.CrewTO;
import oracle.demo.oow.bd.to.MovieTO;

public class MovieUploader {
    /**
     * This method reads the file with MOVIE records and load it into kv-store
     * one movie at a time
     * @throws IOException
     */
    public void uploadMovieInfo() throws IOException {
        FileReader fr = null;
        try {
            fr = new FileReader(Constant.MOVIE_INFO_FILE_NAME);
            BufferedReader br = new BufferedReader(fr);
            String jsonTxt = null;
            MovieTO movieTO = null;
            MovieDAO movieDAO = new MovieDAO();
            int count = 1;

            //Each line in the file is the JSON string

            //Construct MovieTO from JSON object
            while ((jsonTxt = br.readLine()) != null) {
            
            	try {
                    movieTO = new MovieTO(jsonTxt.trim());
                } catch (Exception e) {
                    System.out.println("ERROR: Not able to parse the json string: \t" + jsonTxt);
                }

                if (movieTO != null && !movieTO.isAdult()) {
                   	System.out.println(count++ + " " + movieTO.getMovieJsonTxt());
                    movieDAO.insert(movieTO);
                }
            } //EOF while

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } finally {
            fr.close();
        }


    } //uploadMovies

    /**
     * This method reads the file with MOVIE-CAST records and load it into kv-store
     * one movie at a time
     * @throws IOException
     */
    public void uploadMovieCast() throws IOException {
        FileReader fr = null;
        try {
            fr = new FileReader(Constant.MOVIE_CASTS_FILE_NAME);
            BufferedReader br = new BufferedReader(fr);
            String jsonTxt = null;
            CastTO castTO = null;
            int count = 1;

            //Each line in the file is the JSON string

            //Construct MovieTO from JSON object
            while ((jsonTxt = br.readLine()) != null) {
                try {
                    castTO = new CastTO(jsonTxt.trim());
                } catch (Exception e) {
                    System.out.println("ERROR: Not able to parse the json string: \t" + jsonTxt);
                }

                if (castTO != null) {
                    System.out.println(count++ + " " + castTO.getJsonTxt());
                    CastDAO castDAO = new CastDAO();
                    castDAO.insertCastInfo(castTO);
                } //EOF if
            } //EOF while
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } finally {
            fr.close();
        }


    } //uploadMovies

    /**
     * This method reads the file with movie Crew records and load it into kv-store
     * one movie at a time
     * @throws IOException
     */
    public void uploadMovieCrew() throws IOException {
        FileReader fr = null;
        try {
            fr = new FileReader(Constant.MOVIE_CREW_FILE_NAME);
            BufferedReader br = new BufferedReader(fr);
            String jsonTxt = null;
            CrewTO crewTO = null;
            int count = 1;

            //Each line in the file is the JSON string

            //Construct MovieTO from JSON object
            while ((jsonTxt = br.readLine()) != null) {
                try {
                    crewTO = new CrewTO(jsonTxt.trim());
                } catch (Exception e) {
                    System.out.println("ERROR: Not able to parse the json string: \t" +
                                       jsonTxt);
                }

                if (crewTO != null) {
                    System.out.println(count++ + " " + crewTO.getJsonTxt());
                    CrewDAO crewDAO = new CrewDAO();
                    crewDAO.insertCrewInfo(crewTO);
                } //EOF if

            } //EOF while
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } finally {
            fr.close();
        }


    } //uploadMovieCrew

    /**
     * Run main method to upload movie info, cast & crew information into
     * Oracle NoSQL Database. There are total of six steps defined in this
     * method. Each method loads data from external flat file into the NoSQL
     * database or RDBMS.
     * <p>
     * If TARGET_NOSQL is passed as an argument to the method calls in step 1-4,
     * it means data will be persisted to NoSQL DB, where as when TARGET_RDBMS
     * is passed it means data will be stored in ORA.
     * <p>
     * For Hand-on-Lab you just need to run step 1 & 2 only. You can simply run
     * the main method without passing any argument and that would upload
     * 5000 movies into your NoSQL DB instance
     * <p>
     * If you pass an argument while running this class that would run all six 
     * steps, which means all 200+ thousand movies will be uploaded with all 
     * the cast and crew information, customer profiles will be created and
     * individual movie recommendations will be made for all 100 customer 
     * profiles.
     * @param args
     */
    public static void main(String[] args) {
        MovieUploader mu = new MovieUploader();

        try {
                        
            /**
             * Step 1 - Upload movie info.
             * You can set how many movies do you want to upload into kv-store.
             * There close to quater million movies in all. Default is set to
             * 5000 movies, which if set to 0, would mean upload all the movies.
             */
        	 /*HbaseDB db = HbaseDB.getInstance();
        	 Table table = db.getTable("genre");
        	 
        	 ResultScanner resultScanner = table.getScanner(Bytes.toBytes("genre"));
        	 Iterator<Result> iter = resultScanner.iterator();
        	 while(iter.hasNext()) {
        	 	System.out.println(Bytes.toString(iter.next().getValue(Bytes.toBytes("genre"), Bytes.toBytes("name"))));
        	 }*/
            mu.uploadMovieInfo();
            mu.uploadMovieCast();
        	//mu.uploadMovieCrew();
        } catch (IOException e) {
            e.printStackTrace();
        }
    } //main

}
