package com.cms.exception;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * exception operation module
 * @author lxm
 * @category exception and email
 */
public class ETLException extends Exception {
	
	private static final long serialVersionUID = 1L;
	/**
	 * custom exception module
	 */
	private static SimpleDateFormat sf = new SimpleDateFormat("{yyyy-MM-dd HH:mm:ss}");
	Throwable cause;

	public ETLException() {
		super();
	}

	public ETLException(String msg) {
		super(msg);
	}

	public ETLException(String msg, Throwable cause) {
		super(msg, cause);
	}

	public ETLException(Throwable cause) {
		super(cause);
		this.cause=cause;
	}

	@Override
	public void printStackTrace() {
		//super.printStackTrace();
		sendEmail(cause);
	}

	/**
	 * build exception detail and send to specials email box
	 * @param ex unknownException
	 */
	public void sendEmail(Throwable ex) {
		String message = "";
		StackTraceElement[] st = ex.getStackTrace();
		for (StackTraceElement stackTraceElement : st) {
			String exclass = stackTraceElement.getClassName();
			String method = stackTraceElement.getMethodName();
			message = sf.format(new Date()) + ":" + "[Class:" + exclass + "] Exception occurred in ("
					+ method + ") method at line <" + stackTraceElement.getLineNumber()
					+ ">  exception type:(" + ex.getClass().getName()+")";
		}
		message = message+" Exception Message:**"+ex.getMessage()+"**";
		
		if(!message.contains("java.io.EOFException")){
			SendMailClient.sendTextContent(message);
		}
	}
	
}
