package com.cms.servlet;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;

import com.cms.blacklist.BlackListServer;
import com.cms.db.CMSDBServer;
import com.cms.file.CMSFileServer;
import com.cms.whitelist.WhiteListServer;

/**
 * Servlet implementation class StartTwoServer
 */
//@WebServlet("/StartTwoServer")
public class StartTwoServer extends HttpServlet {
	private static final long serialVersionUID = 1L;
    private CMSFileServer fileServer=null;  
    private BlackListServer blackListServer=null;
    private WhiteListServer whiteListServer=null;
    private CMSDBServer dbServer=null;
    /**
     * @see HttpServlet#HttpServlet()
     */
    public StartTwoServer() {
        super();
    }
    
    @Override
    public void init() throws ServletException {
    	// TODO Auto-generated method stub
    	super.init();
    	System.out.println("=================服务器从Servlet启动======================");
		fileServer = CMSFileServer.getInstance();
		blackListServer = BlackListServer.getInstance();
		whiteListServer = WhiteListServer.getInstance();
		dbServer = CMSDBServer.getInstance();
		
		fileServer.start();
		blackListServer.start();
		whiteListServer.start();
		dbServer.start();
    }
    
    @Override
    public void destroy() {
    	super.destroy();
    	fileServer.stop();
    	blackListServer.stop();
    	whiteListServer.stop();
    	dbServer.stop();
    }
}
