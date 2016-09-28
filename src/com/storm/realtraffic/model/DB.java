package com.storm.realtraffic.model;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import com.bmwcarit.barefoot.logger.MyLogger;
import com.storm.realtraffic.common.Configuration;

public class DB {
	private Connection con = null;
	private Statement stmt = null;
	private ResultSet rs = null;
	public ResultSet selectGPS(){
		con = Postgresql.getConnection();
		if (con == null) {
			MyLogger.logger.error("Failed to make connection!");
			return null;
		}		
		try {
			//选择出租车gps记录表：ValidSampleTable
			stmt = con.createStatement();
			String sql = "select * from " + Configuration.ValidSampleTable; //+ " limit 5000;";
			//String sql = "select * from " + Common.SingleSampleTable + " limit 2000";
			MyLogger.logger.debug(sql);
    		rs = stmt.executeQuery(sql);

    		//start aggregate timer;
    		//TravelTimeAggregate aggregater = new TravelTimeAggregate(15);
    		MyLogger.logger.debug("select finished");
    		return rs;
		}catch(Exception e){
			e.printStackTrace();
		}
		return rs;
	}
}
