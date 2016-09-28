package com.storm.realtraffic.services.spout;

import java.sql.ResultSet;
import java.util.Map;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import com.storm.realtraffic.common.Configuration;
import com.storm.realtraffic.common.Sample;
import com.storm.realtraffic.common.TaxiInfo;
import com.storm.realtraffic.model.DB;

public class RoadSpout implements IRichSpout {
	private static final long serialVersionUID = -5653803832498574836L;
	private SpoutOutputCollector collector;

	ResultSet rs = null;
	static int counter1 = 0;
	public Sample gps = null;
	
	@Override
	public void open(Map args1, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		/////////////////////////////////////////////////////////////////////////////
		Configuration conf = new Configuration();
		conf.init(40000);
		//输出道路信息看一下
		for(int i=0;i<Configuration.roadlist.length;i++){
				System.out.println("roadlist[i].gid:"+Configuration.roadlist[i].gid);
		}

	    DB db =  new DB();
	    rs = db.selectGPS();
	    

	  
	    
	    
	    
	    
	    
    	System.out.println("------------readmap-spout-open-end-------------");
	
    	try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
	}
	
	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		System.out.println("------------readmap-spout-nextTuple-start-------------");
		//模拟gps发送数据
  		try {
  			if(rs.next()){
  				 gps = null;
  				System.out.println(counter1++);	
  				//utc of gps is in the time range, process the point
  				  gps = new Sample(rs.getLong("suid"), rs.getLong("utc"), rs.getLong("lat"), 
  			    		rs.getLong("lon"), (int)rs.getLong("head"), rs.getLong("speed"), rs.getLong("distance"));
  					int suid = (int) gps.suid;			
  					if(Configuration.taxi[suid] == null){
  						Configuration.taxi[suid] = new TaxiInfo();		
  					}
  					Configuration.taxi[suid].add_gps(gps);	
  						collector.emit(new Values(gps));				
  				}
  			
  		}	catch (Exception e) {
  			// TODO Auto-generated catch block
  			e.printStackTrace();
  		}	
		System.out.println("------------readmap-spout-nextTuple-end-------------");
	
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("gps"));
	}
	
	@Override
	public void ack(Object arg0) {
		// TODO Auto-generated method stub
		System.out.println("ack success!");
	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void fail(Object arg0) {
		// TODO Auto-generated method stub
		System.out.println("fail");
	}
	

	public Map<String, Object> getComponentRoadSpout() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
	

	
	
	
	

}
