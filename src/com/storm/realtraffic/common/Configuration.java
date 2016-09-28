package com.storm.realtraffic.common;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import com.bmwcarit.barefoot.matcher.Matcher;
import com.bmwcarit.barefoot.road.PostGISReader;
import com.bmwcarit.barefoot.roadmap.Road;
import com.bmwcarit.barefoot.roadmap.RoadMap;
import com.bmwcarit.barefoot.roadmap.RoadPoint;
import com.bmwcarit.barefoot.roadmap.TimePriority;
import com.bmwcarit.barefoot.spatial.Geography;
import com.bmwcarit.barefoot.topology.Dijkstra;
import com.bmwcarit.barefoot.logger.MyLogger;
import com.bmwcarit.barefoot.util.Tuple;

import com.storm.realtraffic.model.GPSUpdater;
import com.storm.realtraffic.model.RealTrafficUpdater;


public class Configuration {
	
	//some config on database
	public static String JdbcUrl  = "jdbc:postgresql://localhost:5432/";
	public static String Host     = "localhost";
	public static int Port        = 5432;
	public static String UserName = "postgres";
	public static String UserPwd  = "postgres";
	public static String DataBase = "routing";
	public static String OriginWayTable = "ways";
	public static RoadMap map = null;
	
	//config Mapping of road class identifiers to priority factor and default maximum speed
	public static Map<Short, Tuple<Double, Integer>> road_config;
	public static double max_speed = 33.33; //以下所有速度单位都是：米

	public static double min_speed = 0.1;  
	public static double avg_speed = 10;
	
	static double min_interval = 20; //最小时间间隔周期，单位是：秒
	static double speed_alpha = 0.9;
	public static double init_turning_time = 5;
	
	//default speed
	public static  double[][] default_traffic = null;
	//average speed of roads in same class
	public static  double[][] default_class_traffic = null;
	
	public static int match_windows_size = 4;
	public static Matcher matcher;
	
	public static GPSUpdater gps_updater;
	public static GPSUpdater unkown_gps_updater;//points that match failed
	static TravelTimeSliceUpdater traffic_updater;
	public static RealTrafficUpdater real_traffic_updater;
	//to create table with date
	public static String Date_Suffix = "";
	public static String UnKnownSampleTable = "match_fail_gps";
	//current thread number, to control speed of data emission
	public static long max_seg;
	public static String traffic_slice_table ="travel_time_slice_";
	public static String real_turning_slice_table ="real_turning_time_slice_";
	public static String traffic_total_table ="travel_time_total_";
	public static String real_road_slice_table ="real_road_time_slice_";
	
	public static AllocationRoadsegment[] roadlist=null;

	public static long period = 200L;
	public static long end_utc=1270569600L;//1231218000-1231221600
	public static long start_utc=1270483200L;
	
	//send points within next x seconds every time
	public static int emission_step = 1;
	//times of speed of real time.
	public static int emission_multiple = 2;
	
	public static String ValidSampleTable  = "valid_gps_utc";
	public static String SingleSampleTable  = "gps_5434";
	public static String FilterSampleTable = ValidSampleTable;


	public static TaxiInfo taxi[] = null;//taxi sample
	
	
	public static int delay_update_thresold = 1;//to get delay updated traffic
	public static double smooth_alpha = 0.9;//to smooth traffic
	
	

	
	public static int counter = 0;
	
	public static Object obj = new Object();
	
	public static Connection con = null;
	public static Statement stmt = null;
	public static ResultSet rs = null;
	
	
	public  Sample gps = null;
	public void init(int max_suid){
		init_road_config();
		updateMap(max_suid);
		init_roadlist2();
		
	}
	//初始化道路信息
	@SuppressWarnings("unchecked")
	public void init_road_config(){
		road_config = new HashMap<Short, Tuple<Double, Integer>>();
		short class_id[] = {100,101,102,104,105,106,107,108,109,110,111,112,113,114,
				117,118,119,120,122,123,124,125,201,202,301,303,304,305};
		double priority[] = {1.30,1.0,1.10,1.04,1.12,1.08,1.15,1.10,1.20,1.12,1.25,1.30,
				1.50,1.75,1.30,1.30,1.30,1.30,1.30,1.30,1.30,1.30,1.30,1.30,1.30,1.30,1.30,1.30};
		for(int i=0; i<class_id.length; i++){
			road_config.put(class_id[i], new Tuple(priority[i], Configuration.max_speed));
		}
		
		map = RoadMap.Load(new PostGISReader(Host, Port, 
				DataBase, OriginWayTable, UserName, UserPwd, road_config));
		if(map != null){
			map.construct();
		}
		
		//进行gps点与道路匹配
		matcher = new Matcher(map, new Dijkstra<Road, RoadPoint>(),
	            new TimePriority(), new Geography());
		max_seg=(Configuration.end_utc-Configuration.start_utc)/Configuration.period;  //96 
		
	}

	
	

	
	public boolean updateMap(int max_suid){
		try {
			gps_updater = new GPSUpdater(100, "gps_final" + Configuration.Date_Suffix);
			unkown_gps_updater = new GPSUpdater(100, Configuration.UnKnownSampleTable);
			real_traffic_updater = new RealTrafficUpdater();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		taxi = new TaxiInfo[max_suid + 1];
		return true;
	}
	
	
	
public static void init_roadlist2(){
	
		//得到 max_gid
		Iterator<Road> roadmap = map.edges(); //获取所有记录集合
		long max_gid = 0;//max_gid is not equal to size
		while(roadmap.hasNext()){
			Road road = roadmap.next();
			long gid = road.id();
			if(gid > max_gid){
				max_gid = gid;
			}
		}
		MyLogger.logger.debug("max_gid: " + max_gid);
		
		//开辟一个道路对象数组，max_gid的值就是地图一共的道路段数
		roadlist=new AllocationRoadsegment[(int)max_gid]; //开辟了一个对象数组
		for(int i=0;i<roadlist.length;i++){
		    roadlist[i]=new AllocationRoadsegment(); //往对象数组里面添加对象,默认对象
		    roadlist[i].avg_speed = 10;   //设置没一条道路的初始平均速度为10
		}
		
		
		//将数据库中的属性值提取出来，保存到自定义的道路地图数据结构AllocationRoadsegment中
		//对象就是cur_road
		roadmap = map.edges();
		while(roadmap.hasNext()){
			Road road = roadmap.next();
			long gid = road.id();
			double maxspeed = road.maxspeed();
			AllocationRoadsegment cur_road=new AllocationRoadsegment(gid,maxspeed, 10.0, 0);
			cur_road.length = road.length();//meters
			cur_road.time = cur_road.length/10;
			cur_road.base_gid = road.base().id();
			roadlist[(int)gid-1] = cur_road;

			//System.out.println(roadlist[(int)gid-1].length);	
		}	
		
	}
	


}
