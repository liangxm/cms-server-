package com.cms.whitelist;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import com.cms.exception.ETLException;

public class WhiteListServer {

	static Logger log = Logger.getLogger("Server");

	private static Object lock = new Object();

	private static ExecutorService service = null;

	private static int size = 20;
	
	private static WhiteListThread thread = null;
	
	private static WhiteListServer self=null;
	
	private WhiteListServer(){
		
	}
	
	public static WhiteListServer getInstance(){
		synchronized(lock){
			if(self==null){
				self=new WhiteListServer();
			}
		}
		return self;
	}
	
	public void start() {
		log.info("starting WhiteList Server...");
		synchronized(lock) {
			if(!getstatus()){
				if (service == null) {
					service = Executors.newFixedThreadPool(size);
				}
				thread = new WhiteListThread();
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
		log.info("stopping WhiteList Server...");
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
				ReaderWhiteListThread reader = new ReaderWhiteListThread(socket);
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
		WhiteListServer.getInstance().start();
	}


}

