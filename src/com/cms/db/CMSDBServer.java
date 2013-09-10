package com.cms.db;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.cms.util.CMSServerConfig;

public class CMSDBServer {

	private static Object lock = new Object();

	private static ExecutorService service = null;

	private static int size = 1;

	private static DBLoaderThread thread = null;

	static { 
		PropertyConfigurator.configure(CMSServerConfig.getLog4jConfigureURL());
	}
	static Logger log = Logger.getLogger("DBServer");


	/**
	 * @param args
	 */
//	public static void main(String[] args) {
//		// log.info("Database Server Start!");
//		 //start();
//	}
	private static CMSDBServer self=null;
	private CMSDBServer(){
		
	}
	public static CMSDBServer getInstance(){
		synchronized(lock){
			if(self==null){
				self=new CMSDBServer();
			}
		}
		return self;
	}

	public void start() {
		Properties properties = CMSServerConfig.getProperties();

		String path = properties.getProperty("data.home");
		String refreshTime = properties.getProperty("refreshTime");
		log.info("starting Database Server...");
		synchronized (lock) {
			if(!getstatus()){
				if (service == null) {
					service = Executors.newFixedThreadPool(size);
				}
				thread = new DBLoaderThread(path, refreshTime);
				thread.runnable=true;
				service.execute(thread);
			}
		}
	}

	public boolean getstatus() {
		if (thread != null) {
			return thread.runnable;
		} else {
			return false;
		}
	}

	public void stop() {
		log.info("stopping Database Server...");
		synchronized (lock) {
			if(getstatus()){
				if (thread != null) {
					thread.runnable = false;
					synchronized(thread){
						// wake up the waiting thread immediately.
						thread.notifyAll();
					}
				}
				if (service != null) {
					service.shutdown();
					service = null;
				}
			}
		}
	}

}


