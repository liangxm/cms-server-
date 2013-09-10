package com.cms.exception;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.cms.util.ResourceLoader;

/**
 * send email Test
 * @author lxm
 * @time 2013-1-5 12:12:53
 */
public class SendMailClient {       
	private static Properties props = new Properties();
	
	public static void main(String[] args) {           
	} 
	
	static {
		InputStream in = null;
		try {
			in = ResourceLoader.getResource("/email.txt").openStream();
			props.load(in);
			in.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}
	
	public void setInfo(Map<String,String> map){
		OutputStream out = null;
		try {
			out = new FileOutputStream(ResourceLoader.getResource("/email.txt").getFile());
			Set<Map.Entry<String, String>> set = map.entrySet();
			for(Iterator<Map.Entry<String, String>> it = set.iterator();it.hasNext();){
				Map.Entry<String, String> entry = it.next();
				props.setProperty(entry.getKey(), entry.getValue());
			}
			props.store(out, null);
			out.flush();
			out.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static boolean sendTextAndAattchment(String content,String url){
		String server = props.getProperty("SERVER");
		
		String from = props.getProperty("FROM");
		
		String to = props.getProperty("TO");
		
		String username = props.getProperty("USER");
		String password = props.getProperty("PASS");
		
		EMail mail = new EMail(server,
							   from,
							   to,
							   "≤‚ ‘",
							   content,
							   url,
							   username,
							   password);
		SendMail sender = new SendMail(mail);
		
		return sender.send();
	}
	
	public static boolean sendTextContent(String content){
		
		String server = props.getProperty("SERVER");
		
		String from = props.getProperty("FROM");
		
		String to = props.getProperty("TO");
		
		String username = props.getProperty("USER");
		String password = props.getProperty("PASS");
		
		EMail mail = new EMail(server,
							   from,
							   to,
							   "ETL-Exception",
							   content,
							   username,
							   password);
		SendMail sender = new SendMail(mail);
		
		return sender.send();
	}
	
	public static Properties getProps() {
		return props;
	}
}