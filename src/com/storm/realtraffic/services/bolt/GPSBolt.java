package com.storm.realtraffic.services.bolt;

import java.util.ArrayList;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import com.bmwcarit.barefoot.logger.MyLogger;

import com.storm.realtraffic.common.Configuration;
import com.storm.realtraffic.common.Sample;


public class GPSBolt implements IRichBolt  {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2L;
	public  ArrayList<Integer> suid_list =null;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub	
	}
	
	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		// TODO Auto-generated method stub
		
		suid_list =new ArrayList<Integer>();
		
	}

	@Override
	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub

		//ArrayList<Sample> gps_seq = null;
		System.out.println("----------------------gps-bolt-start--------------------");

		
		MyLogger.logger.debug("processing");
		Sample gps = (Sample)tuple.getValue(0);
		System.out.println("---------"+gps.suid);
		put_suid((int)gps.suid);
		System.out.println("suid_list:"+suid_list.size());
		try {
			Object[] temp_suid_list;
			synchronized(suid_list){
				temp_suid_list = suid_list.toArray();
			}
			
			for(int i=0; i< temp_suid_list.length; i++){
				//int suid = suid_list.get(i);
				int suid = (int) temp_suid_list[i];
				Configuration.taxi[suid].process();
				System.out.println("suid:"+suid);
			}
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println("----------------------gps-bolt-stop--------------------");
	
	}
	
	
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}


	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	
	public synchronized  void put_suid(int suid){
		if(suid_list.contains(suid)){
			return;
		}
		synchronized(suid_list){
			suid_list.add(suid);
		}		
	}
	
	
	
}
