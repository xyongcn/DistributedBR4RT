package com.storm.realtraffic.model;


import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.bmwcarit.barefoot.logger.MyLogger;
import com.storm.realtraffic.common.AllocationRoadsegment;
import com.storm.realtraffic.common.Configuration;


//update traffic time and turning time
public class RealTrafficUpdater {
	
	private Connection con = null;
	private Statement stmt = null;
	//private static Lock lock = null;
	
	public RealTrafficUpdater() throws SQLException{
		
		con = Postgresql.getConnection();
		con.setAutoCommit(false);
		try{
			stmt = con.createStatement();
		}
		catch (SQLException e) {
		    e.printStackTrace();
		}
		
		//create all slice table
		for(int i=1; i<= Configuration.max_seg; i++){
			
			try{
				//road time
				String road_slice_table = Configuration.real_road_slice_table + i + Configuration.Date_Suffix;
				String sql = "DROP TABLE IF EXISTS " + road_slice_table + ";";
				stmt.executeUpdate(sql);
				//create slice table
				sql = "CREATE TABLE " + road_slice_table + "(gid integer, base_gid integer,"
						+ " time double precision, average_speed double precision);";
				System.out.println(sql);
				MyLogger.logger.debug(sql);
				stmt.executeUpdate(sql);
				
				//turning time
				String turning_slice_table = Configuration.real_turning_slice_table + i + Configuration.Date_Suffix;
				sql = "DROP TABLE IF EXISTS " + turning_slice_table + ";";
				stmt.executeUpdate(sql);
				//create slice table
				sql = "CREATE TABLE " + turning_slice_table + "(gid integer, next_gid integer,"
						+ " time double precision);";
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
	}
	
	
	
	public boolean update(int gid, int seq) throws SQLException{
		try{			
			//insert road traffic
			String sql = "Insert into " + Configuration.real_road_slice_table + seq + Configuration.Date_Suffix
					+ "(gid, base_gid, time, average_speed) values \n";
			AllocationRoadsegment road = Configuration.roadlist[gid];
			sql += "(" + road.gid + ", " + road.base_gid + ", " + road.time + ", " + road.avg_speed + ");";
			MyLogger.logger.debug(sql);
			stmt.executeUpdate(sql);
			
			//insert turning traffic
			HashMap<Integer, Double> turing_time = road.get_all_turning_time();
			Set<Entry<Integer, Double>> entryset=turing_time.entrySet();
			for(Entry<Integer, Double> m:entryset){
				sql = "Insert into " + Configuration.real_turning_slice_table + seq + Configuration.Date_Suffix
						+ "(gid, next_gid, time) values \n";
				sql += "(" + gid + ", " + m.getKey() + ", " + m.getValue() + ");";
				stmt.addBatch(sql);
			}
			stmt.executeBatch();
		}
		catch (SQLException e) {
		    e.printStackTrace();
		    con.rollback();
		    return false;
		}
		finally{
			con.commit();
		}
		return true;
	}
}
