package com.cms.blacklist;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.cms.exception.ETLException;
import com.cms.util.CMSServerConfig;

public class BlackListThread implements Runnable {

	public boolean runnable = true;
	static Logger log = Logger.getLogger("Server");
	public ServerSocket server = null;
	private SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public void run() {

		try {
			// get properties
			Properties properties = CMSServerConfig.getProperties();
			int port = Integer.parseInt(properties.getProperty("blackport"));
			int timeout = Integer.parseInt(properties.getProperty("timeout"));
			
			// create a socket server
			server = new ServerSocket(port);
			log.info("BlackList Server started port:"+port);
			log.info("ETL BlackList Version "+CMSServerConfig.VERSION);
			
			// running until stopped
			while (runnable) {
				Socket socket = server.accept();
				if (socket == null) {
					continue;
				}
				log.info("connect time:" + sf.format(new Date()) + ";Client Ip:" + socket.getRemoteSocketAddress());
				BlackListServer.startReader(socket,timeout);
			}
		}catch (Exception e) {
			new ETLException(e).printStackTrace();
		} finally {
			if (server != null) {
				try {
					server.close();
				} catch (IOException e) {
					new ETLException(e).printStackTrace();
				}
				server = null;
			}
			runnable=false;
			log.info("BlackList Server Shutdown!");
		}
	}
}

class ReaderBlackListThread implements Runnable  {


	static Logger log = Logger.getLogger("Server");
	private SimpleDateFormat sfDateSync = new SimpleDateFormat("yyyyMMddHHmmss");
	Socket socket = null;
	
	Properties properties = CMSServerConfig.getProperties();
	String path = properties.getProperty("data.home");

	public ReaderBlackListThread(Socket socket) {
		this.socket = socket;
	}

	public void run() {
		log.info("New process request blackList");
		DataInputStream input = null;
		DataOutputStream os = null;
		byte[] buffer = null;
		try {
			input = new DataInputStream(socket.getInputStream());
			os = new DataOutputStream(socket.getOutputStream());
			
			/** get file name length*/
			int fileNameLen = input.readInt();
			buffer = new byte[fileNameLen];
			input.read(buffer);
			String fileName = new String(buffer, CMSServerConfig.CHAR_ENCODE);
			
			/** create a blacklist folder*/
			File dir = new File(path+"/BlackList");
			if (!dir.exists()) {
				dir.mkdirs();
			}
			
			/** create a blacklist special if the needs file not exist*/
			log.info("fileName=====" + fileName);
			File f = new File(path+"/BlackList/" + fileName);
			if(!f.exists()){
				f.createNewFile();
				String initStr = "1111111111111111";
				FileOutputStream out = new FileOutputStream(f);
				out.write(initStr.getBytes());
				out.close();
			}
			
			/** read blacklist file specified from blacklist folder*/
			FileInputStream input2 = new FileInputStream(f);
			int fileLen =input2.available();
			byte[] fileBuffer = new byte[fileLen];
			while (input2.read(fileBuffer) != -1 ) {}
			
			/**calculator how many blacklist card needs to send.*/
			String zzz = new String(fileBuffer);
			String[] zz = zzz.split("[,]");
			int count=0;
			for(String z:zz){
				if(z.length()>0){
					count++;
				}
			}
			log.info("A total of "+ count +" card number was sent.");
			
			byte[] bufferLen = new byte[4];
			bufferLen =	CMSServerConfig.intToByte(fileLen);
			log.info("bufferLen====" + bufferLen);
			
			/**update some code in here for append system time.*/
			byte[] sysTime = sfDateSync.format(new Date()).getBytes();
			
			byte[][] ALL  = {bufferLen,fileBuffer,sysTime};
			int totalLength = fileLen + 4 + sysTime.length;
			byte[] finalAuditData = new byte[totalLength];
			int currentPosition = 0;
			log.info("all.length====" + ALL.length);
			
			for (int i = 0; i < ALL.length; i++) {
				System.arraycopy(ALL[i], 0, finalAuditData, currentPosition, ALL[i].length);
				currentPosition += ALL[i].length;
			}

			/** send blacklist file to client*/
			os.write(finalAuditData);
			input2.close();
		}catch (Exception e) {
			new ETLException(e).printStackTrace();
			
			try{
				if (input!=null) {
					input.close();
				}
				
			}catch(Exception ex){
				new ETLException(ex).printStackTrace();
				log.error(ex.getMessage());
			}
		} finally {
			log.info("---------- End ----------");
			if (input!=null) {
				try {
					input.close();
				} catch (IOException e) {
					log.error(e.getMessage());
				}
				input = null;
			}
			if (os!=null) {
				try {
					os.close();
				} catch (IOException e) {
					log.error(e.getMessage());
				}
				os = null;
			}
			if (socket != null && !socket.isClosed()) {
				try {
					socket.close();
				} catch (IOException e) {
					log.error(e.getMessage());
				}
				socket = null;
			}
		}
	}
}