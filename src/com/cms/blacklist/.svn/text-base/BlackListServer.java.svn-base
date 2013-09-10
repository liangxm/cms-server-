package com.cms.blacklist;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import com.cms.exception.ETLException;

public class BlackListServer {

	static Logger log = Logger.getLogger("Server");

	private static Object lock = new Object();

	private static ExecutorService service = null;

	private static int size = 20;
	
	private static BlackListThread thread = null;
	
	private static BlackListServer self=null;
	
	private BlackListServer(){
		
	}
	
	public static BlackListServer getInstance(){
		synchronized(lock){
			if(self==null){
				self=new BlackListServer();
			}
		}
		return self;
	}
	
	public void start() {
		log.info("starting BlackList Server...");
		synchronized(lock) {
			if(!getstatus()){
				if (service == null) {
					service = Executors.newFixedThreadPool(size);
				}
				thread = new BlackListThread();
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
		log.info("stopping BlackList Server...");
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
				ReaderBlackListThread reader = new ReaderBlackListThread(socket);
				try
				{
					socket.setSoTimeout(timeout);
					
				}catch(Exception ex){
					new ETLException(ex).printStackTrace();
					try
					{
					socket.close();
					}catch(Exception exx){
						new ETLException(exx).printStackTrace();
					}
				}
				service.execute(reader);
			}
		}
	}
	
	public static void main(String[] args) {
		BlackListServer.getInstance().start();
	}


}

