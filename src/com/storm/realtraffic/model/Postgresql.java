package com.storm.realtraffic.model;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.bmwcarit.barefoot.logger.MyLogger;
import com.storm.realtraffic.common.Configuration;



public class Postgresql {
	
	public static Connection getConnection(){
		Connection con=null;
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			MyLogger.logger.error("Where is your PostgreSQL JDBC Driver? " + "Include in your library path!");
			e.printStackTrace();
			return null;
		}
		//System.out.println("PostgreSQL JDBC Driver Registered!");
		try {
			con = DriverManager.getConnection(Configuration.JdbcUrl + Configuration.DataBase, Configuration.UserName, Configuration.UserPwd);
			con.setAutoCommit(false);
			
		} catch (SQLException e) {
			MyLogger.logger.error("Connection Failed! Check output console");
			e.printStackTrace();
			return null;
		}	
		return con;
	}
	
	public static void dropConnection(Connection con){
		try{
			if(con!=null){
				con.close();
			}
		}
		catch (Exception e) {
		    e.printStackTrace();
		}
	}
	
	
	
}
