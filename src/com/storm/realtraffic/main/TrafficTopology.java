package com.storm.realtraffic.main;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

import com.bmwcarit.barefoot.logger.MyLogger;
import com.storm.realtraffic.common.Configuration;
import com.storm.realtraffic.services.bolt.GPSBolt;
import com.storm.realtraffic.services.spout.RoadSpout;



public class TrafficTopology {
	public static void main(String[] args) throws Exception {
		MyLogger.logger.debug("start!");
		SimpleDateFormat tempDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String start_time = tempDate.format(new Date());
		Configuration.Date_Suffix = (new SimpleDateFormat("_yyyy_MM_dd")).format(new Date());	
    	MyLogger.logger.debug("-----Real travel time process start:	"+start_time+"-------!");
    	MyLogger.logger.debug("-----get road info:	-------!");

    	TopologyBuilder builder = new TopologyBuilder();  
    	System.out.println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
  	    builder.setSpout("spout", new RoadSpout());  
  	    builder.setBolt("bolt", new GPSBolt(),9).shuffleGrouping("spout"); 
  	    Config conf = new Config();  
  	    conf.setDebug(true); 
  	    if (args != null && args.length > 0) {
  	      //设置进程数量，即worker数量
  	      conf.setNumWorkers(3);  
  	      //集群模式
  	      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());  
  	    } else {  
  	    	//本地模式
  	    	conf.setMaxTaskParallelism(10);
  	      LocalCluster cluster = new LocalCluster();  
  	      cluster.submitTopology("TrafficTopology", conf, builder.createTopology());
  	    }
    	String end_time = tempDate.format(new Date());		
    	MyLogger.logger.debug("-----Real travel time process finished:	"+end_time+"-------!");
    	MyLogger.logger.debug("process time: " + start_time + " - " + end_time + "counter: " + Configuration.counter);	
		}
}
