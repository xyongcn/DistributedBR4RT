package com.storm.realtraffic.model;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.bmwcarit.barefoot.logger.MyLogger;
import com.storm.realtraffic.common.Configuration;


public class TravelTimeSliceUpdater {
	private ArrayList<String> queue_traffic = null;
	private int update_thresold = 2000;//max size of queue, if exceed, do update
	private int slice_num = 96;
	private Connection con = null;
	private Statement stmt = null;
	private static Lock lock = null;
	
	public boolean addTraffic(ArrayList<String> slice){
		if(slice.isEmpty() || slice == null){
			MyLogger.logger.debug("slice empty or null!");
			return false;
		}
		lock.lock();
		try{
			queue_traffic.addAll(slice);
			MyLogger.logger.debug("number of slice update record: " + queue_traffic.size());
		}
		finally{
			lock.unlock();
		}	
		return true;
		
	}
	
	TravelTimeSliceUpdater(final int update_thresold) throws SQLException{
		this.update_thresold = update_thresold;
		lock = new ReentrantLock();
		queue_traffic = new ArrayList<String>();
		
		con = Postgresql.getConnection();
		con.setAutoCommit(false);
		try{
			stmt = con.createStatement();
		}
		catch (SQLException e) {
		    e.printStackTrace();
		}
		
		//create all slice table
		for(int i=1; i<= slice_num; i++){
			String slice_table = Configuration.traffic_slice_table + i + Configuration.Date_Suffix;
			try{
				String sql = "DROP TABLE IF EXISTS " + slice_table + ";";
				stmt.executeUpdate(sql);
				//create slice table
				sql = "CREATE TABLE " + slice_table + "(seq bigint, gid integer, next_gid integer,"
						+ " time double precision, percent double precision, interval double precision,"
						+ " tmstp bigint, suid bigint, utc bigint, start_pos double precision);";
				MyLogger.logger.debug(sql);
				stmt.executeUpdate(sql);
			}
			catch (SQLException e) {
			    e.printStackTrace();
			    con.rollback();
			}
			finally{
				con.commit();
			}
		}
		
		Timer timer = new Timer();
		timer.schedule(new TimerTask(){
			public void run(){
	            if(queue_traffic.size() > update_thresold){
	            	try{
	            		update();
	            	}
	            	catch (SQLException e) {
	        			e.printStackTrace();
	        		}
	            }       
	        }  
		}, 0, 20000);
	}
	
	private void update() throws SQLException{
		ArrayList<String> copy = new ArrayList<String>();
		lock.lock();
		try{			
			copy.addAll(queue_traffic);
			queue_traffic.clear();
		}
		finally{
			lock.unlock();
		}
		try{
			for(String i : copy){
				stmt.addBatch(i);
			}
			stmt.executeBatch();
			MyLogger.logger.debug("traffic slice updater insert!");
		}
		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			MyLogger.logger.error("update travel slice failed.");
		}
		finally{
			con.commit();
		}
		
	}
	public String toString(){
		String result = "";
		for(String record: queue_traffic){
			result = result + "\n" + record;
		}
		return result;
	}
}
