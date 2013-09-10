package com.cms.servlet;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.cms.exception.SendMailClient;

/**
 * operator form
 * @author lxm
 * @version 2013-1-8 16:06:13
 */
public class SaveEmail extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    public SaveEmail() {
        super();
    }
    
    @Override
    public void init() throws ServletException {
    	super.init();
    	System.out.println("SaveEmail");
    }

	@Override
	protected void service(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		SendMailClient smc = new SendMailClient();
		Map<String,String> map = new HashMap<String,String>();
		PrintWriter out = response.getWriter();
		
		String test = request.getParameter("test");
		if(test!=null){
			if(SendMailClient.sendTextContent("This is a test mail!")){
				out.println("<Script language='javascript'>alert('Test succuess!');window.history.back(-1);</Script>");
			}else{
				out.println("<Script language='javascript'>alert('Test Failure,please check enter!');window.history.back(-1);</Script>");
			}
		}else{
			String server = request.getParameter("server");
			if("gmail".equals(server)){
				server = "smtp.gmail.com";
			}else if("qq".equals(server)){
				server = "smtp.qq.com";
			}else if("163".equals(server)){
				server = "smtp.163.com";
			}
			map.put("SERVER", server);
			map.put("FROM", request.getParameter("asender"));
			map.put("USER", request.getParameter("user"));
			map.put("PASS", request.getParameter("pass"));
			map.put("TO", request.getParameter("to"));
			smc.setInfo(map);
			
			out.println("<Script language='javascript'>alert('information have modified!');top.location='index.jsp';</Script>");
		}
	}
}
