package com.cms.file;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.cms.db.DBPubFuncs;
import com.cms.exception.ETLException;
import com.cms.util.CMSServerConfig;

public class ServerThread implements Runnable {

	public boolean runnable = true;
	static Logger log = Logger.getLogger("Server");
	public ServerSocket server = null;
	private SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public void run() {

		try {
			// get properties
			Socket socket;
			Properties properties = CMSServerConfig.getProperties();
			int port = Integer.parseInt(properties.getProperty("port"));
			int timeout = Integer.parseInt(properties.getProperty("timeout"));
			
			// create a socket server
			server = new ServerSocket(port);
			log.info("Server started port:"+port);
			log.info("ETL Version "+CMSServerConfig.VERSION);
			
			// running until stopped
			while (runnable) {
				  socket = server.accept();
				if (socket == null) {
					continue;
				}
				log.info("connect time:" + sf.format(new Date()) + ";Client Ip:" + socket.getRemoteSocketAddress());
				CMSFileServer.startReader(socket,timeout);
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
			log.info("File Server Shutdown!");
		}
	}
}

class ReaderThread implements Runnable  {


	static Logger log = Logger.getLogger("Server");

	Socket socket = null;
	
	// get properties
	Properties properties = CMSServerConfig.getProperties();
	String path = properties.getProperty("data.home");
	
	private SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private SimpleDateFormat sfDateSync = new SimpleDateFormat("yyyyMMddHHmmss");

	public ReaderThread(Socket socket) {
		this.socket = socket;
	}

	public void run() {
		log.info("New process request");
		DataInputStream input = null;
		FileOutputStream output = null;
		OutputStream os = null;
		String Tempname ="";
		byte[] buffer = null;
		DBPubFuncs dbpub = new DBPubFuncs();
		ResultSet rs = null;
		File dir = new File(path);
		if (!dir.exists()) {
			dir.mkdirs();
		}

		try {
			input = new DataInputStream(socket.getInputStream());
			os = socket.getOutputStream();

			buffer = new byte[2];
			input.read(buffer);
			
			
			if (Arrays.equals(CMSServerConfig.END_TAG, buffer)) {
				socket.close();
				log.info("Download Black List was done");
				return;
			}
			else if (!Arrays.equals(CMSServerConfig.START_TAG, buffer)) {
				log.error("Invalid file stream at start tag!");
				return;
			}
			int fileNameLen = input.readInt();
			buffer = new byte[fileNameLen];
			input.read(buffer);

			String fileName = new String(buffer, CMSServerConfig.CHAR_ENCODE);
			String initFileName = "Error_" + fileName;
			
			//initialize incoming to file status table,the file status equals 01(01 means receive failure)
			rs = dbpub.execQuery("select * from tb_file_status where fileName in ('" + initFileName + "','" + fileName + "')");
			String initialTime = sf.format(new Date());
			if(!rs.next()){
				String sql = "insert into tb_file_status values('" + initFileName
						+ "','01','" + initialTime + "','" + initialTime + "')";
				String result = dbpub.exec_str(sql);
				if(result.length() > 0){
					log.info("insert into db_file_status error");
					throw new Exception("insert into db_file_status error");
				}
			}else{
				String sql2 = "update tb_file_status set status='01',fileName='" + initFileName + "' where fileName in ('" + initFileName + "','" + fileName + "')";
				String result = dbpub.exec_str(sql2);
				if(result.length() > 0){
					log.info("records exist .... update db_file_status error");
					throw new Exception("records exist .... update db_file_status error");
				}
			}
			
			log.info("init file status----fileName:" + initFileName + ",status:01----init success");
			if (!(fileName.substring(fileName.length()-4,fileName.length()).equalsIgnoreCase(".csv")||fileName.substring(fileName.length()-4,fileName.length()).equalsIgnoreCase(".zip"))) {
				throw new Exception("Invalid file type! Should be csv or zip!");
			}

			Tempname = initFileName;
			if(fileName.contains("_BlackList.csv")){
				dir = new File(path+ "/BlackList");
				if (!dir.exists()) {
					dir.mkdirs();
				}
			}
			
			//TODO about whitelist for redmine #977
			if(fileName.contains("_WhiteList.csv")){
				dir = new File(path+ "/WhiteList");
				if(!dir.exists()){
					dir.mkdirs();
				}
			}
			//added end

			if(fileName.equalsIgnoreCase("avatar.zip")){
				dir = new File(path+ "/AvatarBak");
				if (!dir.exists()) {
					dir.mkdirs();
				}
			}

			output = new FileOutputStream(new File(dir, initFileName));
			int total = input.readInt();
			log.info("FileSize: " + total + " Bytes");

			int size = 1024;
			byte[] bufferOutput = new byte[size];
			int len = 0;

			int fileSize = 0;
			int clientFileSize = total;
			int card_total = 0;
			while (total > 0) {
				//calculator input stream size
				len = total > size ? size : total;
				
				len = input.read(bufferOutput, 0, len);
				if (len == -1) {
					log.error("Invalid file stream while reading!");
					throw new Exception("the file size is not right.");
				}
				fileSize = fileSize + len;
				output.write(bufferOutput, 0, len);
				total -= len;
				
				if(fileName.contains("_BlackList.csv") || fileName.contains("_WhiteList.csv")){
					String zzz = new String(bufferOutput);
					String[] zz = zzz.split("[,]");
					for(String z:zz){
						if(z.length()>0){
							card_total++;
						}
					}
				}
			}
			output.close();
			
			//file input stream size
			log.info("real file Size===" + fileSize);
			//compare incoming file size and real incoming file size
			if(clientFileSize != fileSize){
				log.info("file size is not same");
				os.write(new String("E003").getBytes());
				// add by hans for time sync
				os.write(sfDateSync.format(new Date()).getBytes());
				throw new Exception("the file size is not same");
			}
			File file = new File(dir,initFileName);
			log.info("----- Get File Successful! -----");
			
			
			buffer = new byte[2];
			input.read(buffer);
			if (!Arrays.equals(CMSServerConfig.END_TAG, buffer)) {
				throw new Exception("Invalid file stream at end tag!");
			}
			os.write(new String("I003").getBytes());
			// add by hans for time sync
			os.write(sfDateSync.format(new Date()).getBytes());
			
			if(fileName.contains("_BlackList.csv") || fileName.contains("_WhiteList.csv")){
				if(new File(dir,fileName).exists()){
					new File(dir,fileName).delete();
				}
				log.info("A total of "+ card_total +" card number was received.");
			}
			file.renameTo(new File(dir,fileName));
			log.info("FileName: " + fileName);
			
			//the file receive successful modify status equals 02
			String updateTime = sf.format(new Date());
			String sql2 = "update tb_file_status set fileName='" + fileName + "',"
					+ "status='02',updateTime='" + updateTime + "' where fileName='" + initFileName + "'";
			
			String result2 = dbpub.exec_str(sql2);
			if(result2.length() > 0){
				log.info("update db_file_status filename=" + initFileName + "error");
				throw new Exception("update db_file_status error");
			}
			log.info("update file " + fileName + ",status:02----receive success");
			rs.close();
			input.close();
		}catch (Exception e) {
			log.error("Failed Cause by "+e.getMessage());
			new ETLException(e).printStackTrace();
			//
			if(os != null){
				try {
					os.write(new String("E003").getBytes());
					// add by hans for time sync
					os.write(sfDateSync.format(new Date()).getBytes());

				} catch (IOException e1) {
					log.error(e.getMessage());
					new ETLException(e).printStackTrace();
				}
			}else{
				log.info("socket connect has problem");
			}
			try{
				if (input!=null) {
					input.close();
				}
				if (output!=null) {
					output.close();
				}
			}catch(Exception ex){
				log.error(ex.getMessage());
				new ETLException(e).printStackTrace();
			}
			File badfile = new File(dir, Tempname);
			if(badfile.exists()){
				//badfile.delete();
			}
		} finally {
			log.info("---------- End ----------");

			try {
				if(rs != null)
					rs.close();
			} catch (SQLException e) {
				new ETLException(e).printStackTrace();
			}
			//close database connection
			dbpub.freeConn();

			if (input!=null) {
				try {
					input.close();
				} catch (IOException e) {
					log.error(e.getMessage());
					new ETLException(e).printStackTrace();
				}
				input = null;
			}
			if (output!=null) {
				try {
					output.close();
				} catch (IOException e) {
					log.error(e.getMessage());
					new ETLException(e).printStackTrace();
				}
				output = null;
			}
			if (socket != null && !socket.isClosed()) {
				try {
					socket.close();
				} catch (IOException e) {
					log.error(e.getMessage());
					new ETLException(e).printStackTrace();
				}
				socket = null;
			}
		}
	}
}