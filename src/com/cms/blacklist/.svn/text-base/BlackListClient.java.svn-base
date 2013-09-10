package com.cms.blacklist;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import com.cms.exception.ETLException;
import com.cms.util.CMSServerConfig;
import com.cms.util.FileUtilities;

public class BlackListClient {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		ExecutorService exec = Executors.newCachedThreadPool(); 
		final Semaphore semp = new Semaphore(50);
		
		for (int index = 0; index<1; index++) {
			final int NO = index;

			Runnable run = new Runnable() {

			public void run() {
				try {

					// 获取许可

					semp.acquire();

					System.out.println("Thread:" + NO);
					BlackListClient client = new BlackListClient();
					client.sendFile("E:\\test\\Lippo_BlackList.csv");
					
					System.out.println("Release：thread" + NO );

					semp.release();

				}catch (Exception e) {

					new ETLException(e).printStackTrace();

				}

			}
			
			};

			exec.execute(run);
		}
		
		exec.shutdown();
//		client.sendFile("test.csv");
//		CMSFileClient client2 = new CMSFileClient();
//		client2.sendFile("test.csv");
//		client2.sendFile("test.csv");
//		client2.sendFile("test.csv");
//		client2.sendFile("test.csv");
		
	}
	

	private void sendFile(String filePath) {
		File file = new File(filePath);
		if (!file.exists()) {
			return;
		}
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssms");
		Socket client = null;
		DataOutputStream output = null;
		FileInputStream input = null;
		int size = 1024;
		byte[] buffer = new byte[size];
		int len = 0;
		try {
			// get properties
//			Properties properties = CMSServerConfig.getProperties();
//			String host =properties.getProperty("host");
//			int port = Integer.parseInt(properties.getProperty("port"));
			// create socket client
//			client = new Socket("122.248.247.149", 3030);
			
			client = new Socket("127.0.0.1", 3036);
			output = new DataOutputStream(client.getOutputStream());

			// load file information
			input = new FileInputStream(file);
			//byte[] fileName = (sdf.format(new Date())+".csv").toString().getBytes(CMSServerConfig.CHAR_ENCODE);
			byte[] fileName = file.getName().getBytes(CMSServerConfig.CHAR_ENCODE);
			int fileLength = input.available();
			System.out.println("file length - " + fileLength);
			System.out.println("file name-" + fileName);
			// write start tag
			//output.write(CMSServerConfig.START_TAG);
			// write length of file name
			output.writeInt(fileName.length);
			// write file name
			output.write(fileName);
			// write size of file
			//output.writeInt(fileLength);
			
			// write buffered file content
//			while ((len = input.read(buffer)) != -1) {
//				output.write(buffer, 0, len);
//			}
//			
//			// write end tag
//			output.write(CMSServerConfig.END_TAG);
			
			BufferedReader br = new BufferedReader(new InputStreamReader(client.getInputStream()));

			
			String temp = "";
			String s = "";
			while((temp = br.readLine()) != null)
			{
			  s = s + temp;
			}
			System.out.println(s);
			String result = s;
			
			FileUtilities fu = new FileUtilities();
			fu.newFile("D:\\test\\Lippo_BlackList.csv", s);
			
			if(result.equalsIgnoreCase("ok")){
				if (input != null) {
					try {
						input.close();
					} catch (IOException e) {
						new ETLException(e).printStackTrace();
					}
					input = null;
				}
				if (output != null) {
					try {
						output.close();
					} catch (IOException e) {
						new ETLException(e).printStackTrace();
					}
					output = null;
				}
				if (client != null) {
					try {
						client.close();
					} catch (IOException e) {
						new ETLException(e).printStackTrace();
					}
					client = null;
				}
				
			}
			
			
		} catch (Exception e) {
			new ETLException(e).printStackTrace();
		}
	}

}
