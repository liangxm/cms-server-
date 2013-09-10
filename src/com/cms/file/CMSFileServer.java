package com.cms.file;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.cms.exception.ETLException;
import com.cms.util.CMSServerConfig;

public class CMSFileServer {
	static { 
		PropertyConfigurator.configure(CMSServerConfig.getLog4jConfigureURL()); 
	}
	static Logger log = Logger.getLogger("Server");
	private static Object lock = new Object();

	private static ExecutorService service = null;

	private static int size = 20;
	
	private static ServerThread thread = null;
	
	private static CMSFileServer self;
	private CMSFileServer(){
		
	}

	public static CMSFileServer getInstance(){
		synchronized(lock){
			if(self==null){
				self=new CMSFileServer();
			}
		}
		return self;
	}

	public  void start() {
		log.info("reload log4j configuration.");
		PropertyConfigurator.configure(CMSServerConfig.getLog4jConfigureURL()); 
		log.info("starting File Server...");
		synchronized(lock) {
			if(!getstatus()){
				if (service == null) {
					service = Executors.newFixedThreadPool(size);
				}
				thread = new ServerThread();
				thread.runnable=true;
				service.execute(thread);
			}
		}
	}

	public boolean getstatus(){
			if (thread != null) {
				return thread.runnable;
			}else{
				return false;
			}
	}
	public  void stop() {
		log.info("stopping File Server...");
		synchronized(lock) {
			if(getstatus()){
				if (thread != null) {
					thread.runnable = false;
						try {
							if(!thread.server.isClosed()){
								thread.server.close();
							}
						} catch (IOException e) {
							new ETLException(e).printStackTrace();
						}
				}
				if (service != null) {
					service.shutdown();
					service = null;
				}
			}
		}
	}

	public static void startReader(Socket socket,int timeout) {
		synchronized(lock) {
			if (service != null) {
				ReaderThread reader = new ReaderThread(socket);
				try
				{
					socket.setSoTimeout(timeout);
					
				}catch(Exception ex){
					try
					{
					socket.close();
					}catch(Exception exx){};
				}
				service.execute(reader);
			}
		}
	}
	
//	public static void stopReader(Socket socket) {
//		synchronized(lock) {
//			if (service != null) {
//				ReaderThread reader = new ReaderThread(socket);
//				service.execute(reader);
//				
//			}
//		}
//	}
	public static void main(String[] args) {
		CMSFileServer.getInstance().start();
	}

}

