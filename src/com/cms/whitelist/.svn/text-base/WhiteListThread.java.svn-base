package com.cms.whitelist;

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

public class WhiteListThread implements Runnable {

	public boolean runnable = true;
	static Logger log = Logger.getLogger("Server");
	public ServerSocket server = null;
	private SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public void run() {

		try {
			// get properties
			Properties properties = CMSServerConfig.getProperties();
			int port = Integer.parseInt(properties.getProperty("whiteport"));
			int timeout = Integer.parseInt(properties.getProperty("timeout"));
			
			// create a socket server
			server = new ServerSocket(port);
			log.info("WhiteList Server started port:"+port);
			log.info("ETL WhiteList Version "+CMSServerConfig.VERSION);
			
			// running until stopped
			while (runnable) {
				Socket socket = server.accept();
				if (socket == null) {
					continue;
				}
				log.info("connect time:" + sf.format(new Date()) + ";Client Ip:" + socket.getRemoteSocketAddress());
				WhiteListServer.startReader(socket,timeout);
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

class ReaderWhiteListThread implements Runnable  {


	static Logger log = Logger.getLogger("Server");
	Socket socket = null;
	
	Properties properties = CMSServerConfig.getProperties();
	String path = properties.getProperty("data.home");

	public ReaderWhiteListThread(Socket socket) {
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
			
			/** create a white list folder*/
			File dir = new File(path+"/WhiteList");
			if (!dir.exists()) {
				dir.mkdirs();
			}
			
			/** create a white list special if the needs file not exist*/
			log.info("fileName=====" + fileName);
			File f = new File(path+"/WhiteList/" + fileName);
			if(!f.exists()){
				f.createNewFile();
				String initStr = "1111111111111111";
				FileOutputStream out = new FileOutputStream(f);
				out.write(initStr.getBytes());
				out.close();
			}
			
			/** read white list file specified from blacklist folder*/
			FileInputStream input2 = new FileInputStream(f);
			int fileLen =input2.available();
			byte[] fileBuffer = new byte[fileLen];
			while (input2.read(fileBuffer) != -1 ) {}
			
			/**calculator how many white list card needs to send.*/
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
			
			byte[][] ALL  = {bufferLen,fileBuffer};
			int totalLength = fileLen + 4;
			byte[] finalAuditData = new byte[totalLength];
			int currentPosition = 0;
			log.info("all.length====" + ALL.length);
			
			for (int i = 0; i < ALL.length; i++) {
				System.arraycopy(ALL[i], 0, finalAuditData, currentPosition, ALL[i].length);
				currentPosition += ALL[i].length;
			}

			/** send white list file to client*/
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