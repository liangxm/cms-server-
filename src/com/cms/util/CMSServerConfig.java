package com.cms.util;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

import com.cms.exception.ETLException;

public class CMSServerConfig {

	public static String CHAR_ENCODE = "UTF-8";
//	public final static String VERSION = "2.0.9";
	public final static String VERSION = "1.7.1 2013-03-06";
	public static byte[] START_TAG = new byte[] {
			(byte) 0xC0,
			(byte) 0xC1
	};
	public static byte[] END_TAG = new byte[] {
			(byte) 0xD0,
			(byte) 0xD1
	};
	
	public static boolean isLinux = true;
	private static  String configPath = "";
	

	public static URL getLog4jConfigureURL() {
		URL log4jConfigureURL= ResourceLoader.getResource("/log4j.properties");
		System.out.println("log4jConfigureURL:"+log4jConfigureURL);
			return log4jConfigureURL;
	}
	public static String getLog4jConfigure() {
		String log4jConfigure= ResourceLoader.getResource("/log4j.properties").getFile();
		System.out.println("log4jConfigure:"+log4jConfigure);
			return log4jConfigure;
	}

	public static String getConfigPath() {
		URL urlConfig;
		// search the customize properties file for current user
		System.out.println("user.name="+System.getProperty("user.name"));
		urlConfig=ResourceLoader.getResource("/application_"+System.getProperty("user.name")+".properties");
		if(urlConfig==null){
			urlConfig=ResourceLoader.getResource("/application.properties");
		}
		configPath= urlConfig.toString();
		//configPath =""+ ResourceLoader.getResource("/application.properties");
		//  System.getProperty( "user.dir" )+"/WEB-INF/classes/application.properties";
		/*
		if(isLinux)
			configPath = "/opt/apache-tomcat-7.0.21/webapps/WebServer/WEB-INF/classes/application.properties";
			//configPath = "/opt/Tomcat7/webapps/WebServer_QA/WEB-INF/classes/application.properties";		    
		else
			configPath = "C:/Tomcat7/webapps/WebServer_WIN/WEB-INF/classes/application.properties";
			//configPath = "E:\\project\\CMSServer\\WebServer_WIN\\src\\applicationLocal.properties";
		
		//*/
		System.out.println("configPath"+configPath.substring(5));
			return configPath.substring(5);
	}

	// added by Hans for space in the path of webapps will cause load failure.
	public static URL getConfigPathURL() {
		URL urlConfig;
		// search the customize properties file for current user
		System.out.println("user.name="+System.getProperty("user.name"));
		urlConfig=ResourceLoader.getResource("/application_"+System.getProperty("user.name")+".properties");
		if(urlConfig==null){
			urlConfig=ResourceLoader.getResource("/application.properties");
		}
		configPath= urlConfig.toString();
		return urlConfig;
	}
	// added end

	public static  Properties getProperties() {
		
//		if(isLinux)
//			configPath = "/opt/apache-tomcat-7.0.21/webapps/WebServer/WEB-INF/classes/application.properties";
//			//configPath = "/opt/Tomcat7/webapps/WebServer_QA/WEB-INF/classes/application.properties";
//		else
//			configPath = "C:/Tomcat7/webapps/WebServer_WIN/WEB-INF/classes/application.properties";
//			//configPath = "E:\\project\\CMSServer\\WebServer_WIN\\src\\applicationLocal.properties";
		
		Properties properties = new Properties();
		InputStream input = null;
		try {
//			input = new FileInputStream(getConfigPath());
			input = getConfigPathURL().openStream();
			properties.load(input);
		} catch (Exception e) {
			new ETLException(e).printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					new ETLException(e).printStackTrace();
				}
				input = null;
			}
		}
		return properties;
	}
	
	public static byte[] intToByte(int i) {
        byte[] abyte0 = new byte[4];
        abyte0[0] = (byte) (0xff & i);
        abyte0[1] = (byte) ((0xff00 & i) >> 8);
        abyte0[2] = (byte) ((0xff0000 & i) >> 16);
        abyte0[3] = (byte) ((0xff000000 & i) >> 24);
        return abyte0;
    }

    public  static int bytesToInt(byte[] bytes) {
        int addr = bytes[0] & 0xFF;
        addr |= ((bytes[1] << 8) & 0xFF00);
        addr |= ((bytes[2] << 16) & 0xFF0000);
        addr |= ((bytes[3] << 24) & 0xFF000000);
        return addr;
    }


}
