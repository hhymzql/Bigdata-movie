<%@ page import="java.util.HashMap" %>
<%@ page import="java.util.Random" %>
<%@ page import="oracle.demo.oow.bd.util.KeyUtil" %>
<%@ page import="oracle.demo.oow.bd.util.YouTubeUtil" %>
<%@ page import="oracle.demo.oow.bd.to.ActivityTO" %>
<%@ page import="oracle.demo.oow.bd.hbase.dao.ActivityDAO" %>
<%@ page import="oracle.demo.oow.bd.pojo.ActivityType" %>
<%
	String movieId = request.getParameter("id");
	int userId = (Integer)request.getSession().getAttribute("userId");
	System.out.println("GetPlayer:movieID="+movieId+"userId="+userId);
	if (movieId != ""){
		ActivityDAO activityDAO = new ActivityDAO();
	    ActivityTO activityTO = activityDAO.getActivityTO(userId, Integer.parseInt(movieId));
	    int position = 0;
	    if(activityTO!=null) 
	    	position = activityTO.getPosition();
	    
	    activityTO.setActivity(ActivityType.STARTED_MOVIE);
	    activityDAO.insertCustomerActivity(activityTO);
	    	    
	    String youtubeKey = YouTubeUtil.getKey(Integer.parseInt(movieId)); 
    %>
<!--
<script type="text/javascript">$(document).ready(function(){player.pauseVideo();player.loadVideoById("<%= youtubeKey %>", <%= position %>, "");});</script>
-->
<script type="text/javascript">loadYTVideo("<%= youtubeKey %>",<%= position %>);</script>
<%
} %>