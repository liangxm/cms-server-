package com.cms.exception;
import java.util.Properties;   

import javax.activation.DataHandler;
import javax.activation.FileDataSource;
import javax.mail.BodyPart;
import javax.mail.Message; 
import javax.mail.Multipart;
import javax.mail.Session; 
import javax.mail.Transport; 
import javax.mail.internet.InternetAddress; 
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;   
import javax.mail.internet.MimeMultipart;

/**
 * send email
 * @author lxm
 * @time 2013-1-5 12:12:53
 */
public class SendMail {       
	/** mail and mail server information*/
	private EMail email;
	
	/** email service object*/
	private MimeMessage mimeMsg;
	private Multipart mp;
	private Properties props;
	
	/** constructor with a email object*/
	public SendMail(EMail email){         
		this.email = email;
		setSmtpHost(email.getHostname());
		createMimeMessage();
	}
	
	/** set Sender's Server information*/
	public void setSmtpHost(String hostName) {
		System.out.println("Set System properties£ºmail.smtp.host = " + hostName);
		
		if (props == null)
			props = System.getProperties();

		props.put("mail.smtp.host", hostName); 
		
		/** set additional information if the server is google server*/
		String[] temp = new String[3];
		temp = hostName.split("[.]");
		System.out.println("Server:"+temp[1]);
		if(temp[1].equals("gmail")){
			props.put("mail.debug", "false"); 
			props.put("mail.smtp.auth", "true"); 
			props.put("mail.smtp.starttls.enable","true"); 
			props.put("mail.smtp.EnableSSL.enable","true");
			props.setProperty("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");   
			props.setProperty("mail.smtp.socketFactory.fallback", "false");   
			props.setProperty("mail.smtp.port", "465");  
		}
	}
	
	/** create and initialize service object*/
	public boolean createMimeMessage() {
		Session session = null;
		try {
			System.out.println("Ready to get the mail session object!");
			session = Session.getDefaultInstance(props, null);
		} catch (Exception e) {
			System.err.println("Get email session object error has occurred!" + e);
			return false;
		}

		System.out.println("Ready to create MIME email object!");
		try {
			mimeMsg = new MimeMessage(session);
			mp = new MimeMultipart();
			return true;
		} catch (Exception e) {
			System.err.println("create MIME email object failure£¡" + e);
			return false;
		}
	}
	
	/** set need authenticate */ 
	public void setNeedAuth(boolean need) {
		System.out.println("set smtp authentication£ºmail.smtp.auth = " + need);
		if (props == null)
			props = System.getProperties();

		if (need) {
			props.put("mail.smtp.auth", "true");
		} else {
			props.put("mail.smtp.auth", "false");
		}
	}
	
	/** set email subject which will send */
	public boolean setSubject(String mailSubject) {
		System.out.println("set email subject£¡");
		try {
			mimeMsg.setSubject(mailSubject);
			return true;
		} catch (Exception e) {
			System.err.println("set email subject error ocurred£¡");
			return false;
		}
	}
	
	/** set email content which will send*/
	public boolean setBody(String mailBody) {
		try {
			BodyPart bp = new MimeBodyPart();
			bp.setContent("" + mailBody, "text/html;charset=GB2312");
			mp.addBodyPart(bp);

			return true;
		} catch (Exception e) {
			System.err.println("An error occurred while setting body of the message£¡" + e);
			return false;
		}
	}
	
	/** set acceptor */
	public boolean setTo(String to) {
		if (to == null)
			return false;
		try {
			mimeMsg.setRecipients(Message.RecipientType.TO,
					InternetAddress.parse(to));
			return true;
		} catch (Exception e) {
			return false;
		}
	}
	
	/** set email address sender*/ 
	public boolean setFrom(String from) {
		System.out.println("Setting sender£¡");
		try {
			mimeMsg.setFrom(new InternetAddress(from)); 
			return true;
		} catch (Exception e) {
			return false;
		}
	}
	
	/** set attachment of the email*/
	public boolean addFileAffix(String filename) {
		System.out.println("Add email attachment£º" + filename);
		try {
			BodyPart bp = new MimeBodyPart();
			FileDataSource fileds = new FileDataSource(filename);
			bp.setDataHandler(new DataHandler(fileds));
			bp.setFileName(fileds.getName());

			mp.addBodyPart(bp);
			return true;
		} catch (Exception e) {
			System.err.println("Add email attachment£º" + filename + "has error occurred£¡" + e);
			return false;
		}
	}
	
	/** start send email*/
	public boolean sendout(String name,String pass) {
		try {
			mimeMsg.setContent(mp);
			mimeMsg.saveChanges();
			System.out.println("Sending message....");

			Session mailSession = Session.getInstance(props, null);
			Transport transport = mailSession.getTransport("smtp");
			transport.connect((String) props.get("mail.smtp.host"), name,
					pass);
			transport.sendMessage(mimeMsg,
					mimeMsg.getRecipients(Message.RecipientType.TO));

			System.out.println("Send message success£¡");
			transport.close();

			return true;
		} catch (Exception e) {
			System.err.println("Send message failure£¡" + e);
			return false;
		}
	} 
	
	/**
	 * @return boolean
	 * 	true: success
	 *  false:error
	 */
	public boolean send(){
		setNeedAuth(true);
		if(!setSubject(email.getSubject())){
			return false;
		}else if(!setBody(email.getText())){
			return false;
		}else if(!setTo(email.getTo())){
			return false;
		}else if(!setFrom(email.getFrom())){
			return false;
		}else if(email.getAttachment()!=null && !addFileAffix(email.getAttachment())){
			return false;
		}else if(!sendout(email.getUsername(),email.getPassword())){
			return false;
		}
		return true;
	}
}