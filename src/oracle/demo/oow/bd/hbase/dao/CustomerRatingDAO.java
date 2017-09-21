package oracle.demo.oow.bd.hbase.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import oracle.demo.oow.bd.hbase.dao.MovieDAO;
import oracle.demo.oow.bd.hbase.mysql.DBUtil;
import oracle.demo.oow.bd.to.MovieTO;


/**
 * This class is used to access recommended movie data for customer
 */
public class CustomerRatingDAO{

    public void insertCustomerRating(int userId, int movieId, int rating) {
    	
    	DBUtil dbUtil = new DBUtil();
 	    String insert = null;
 	    PreparedStatement stmt = null;
 	    Connection conn = dbUtil.getConn();
 	    insert = "Insert into CUST_RATING(userID,movieID,rating) values(?,?,?)";
        if(conn!=null){
     	   try {
			stmt = conn.prepareStatement(insert);
			stmt.setInt(1, userId);
	     	stmt.setInt(2, movieId);
	     	stmt.setInt(3, rating);
	     	stmt.execute();
	     	stmt.close();  	
     	   } catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
     	   }
        }
   
    	/*HbaseDB db = HbaseDB.getInstance();
    	Table table = db.getTable(ConstantsHBase.TABLE_ACTIVITY);
    	Long id = db.getId("gid","gid","activity_id");
        Put put = new Put(Bytes.toBytes(id));
        put.addColumn(Bytes.toBytes("activity"), Bytes.toBytes("user_id"), Bytes.toBytes(userId));
        put.addColumn(Bytes.toBytes("activity"), Bytes.toBytes("movie_id"), Bytes.toBytes(movieId));
        put.addColumn(Bytes.toBytes("activity"), Bytes.toBytes("rating"), Bytes.toBytes(rating));
    
        try {
			table.put(put);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
    	
    }


    public void deleteCustomerRating(int userId) {
        String delete = null;
        DBUtil dbUtil = new DBUtil();
        PreparedStatement stmt = null;
        Connection conn = dbUtil.getConn();
        delete = "DELETE FROM CUST_RATING WHERE USERID = ?";
        try {
            if (conn != null) {
                stmt = conn.prepareStatement(delete);
                stmt.setInt(1, userId);
                stmt.execute();
                stmt.close();
            }
        } catch (SQLException e) {
            System.out.println(e.getErrorCode() + ":" + e.getMessage());
        }
    }

    public List<MovieTO> getMoviesByMood(int userId) {
        List<MovieTO> movieList = null;
        String search = null;
        DBUtil dbUtil = new DBUtil();
        PreparedStatement stmt = null;
        Connection conn = dbUtil.getConn();
        ResultSet rs = null;
        MovieTO movieTO = null;
        MovieDAO movieDAO = new MovieDAO();
        String title = null;
        Hashtable<String, String> movieHash = new Hashtable<String, String>();

        search = "select * from movie_similarity where mid1 = ? and cor > 4";  
        try {
            if (conn != null) {
                //initialize movieList only when connection is successful
                movieList = new ArrayList<MovieTO>();
                stmt = conn.prepareStatement(search);
                stmt.setInt(1, userId);
                rs = stmt.executeQuery();
                while (rs.next()) {
                    //Retrieve by column name
                    int id = rs.getInt("mid2");

                    //create new object
                    movieTO = movieDAO.getMovieById(id);
                    if (movieTO != null) {
                        title = movieTO.getTitle();
                        //Make sure movie title doesn't exist before in the movieHash
                        if (!movieHash.containsKey(title)) {
                            movieHash.put(title, title);
                            movieList.add(movieTO);
                        }
                    } //if (movieTO != null)
                } //EOF while
            } //EOF if (conn!=null)
        } catch (Exception e) {
            //No Database is running, so can not recommend item-item similarity             
        }
        return movieList;
    }
}
