package com.cms.db;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.cms.exception.ETLException;
import com.cms.util.CMSServerConfig;

/**
 * database operation
 */
public class DBPubFuncs {

	private Connection conn;
	private Statement stmt;
	private ResultSet rs;
	
	static Logger log = Logger.getLogger("DBServer");
    

	public DBPubFuncs() {
		createConnection();
	}


	/**
	 * use config file, connect database
	 * 
	 */

	private void createConnection() {
		conn = null;
		stmt = null;
		Properties properties = CMSServerConfig.getProperties();
		String JDBC_DRIVER = properties.getProperty("JDBC_DRIVER");
		String JDBC_URL = properties.getProperty("JDBC_URL");
		String DB_USERNAME = properties.getProperty("JDBC_USER");
		String DB_PASSWORD = properties.getProperty("JDBC_PASS");
		try {
			
			Class.forName(JDBC_DRIVER).newInstance();
			
			conn = DriverManager.getConnection(JDBC_URL,
					DB_USERNAME, DB_PASSWORD);
			stmt = conn.createStatement();
		} catch (Exception e) {
			log.error("建立连接错误原因" + e.toString());
			new ETLException(e).printStackTrace();
		}
	}

	public int exec_getRowNum(String sql) {
		log.debug(sql);
		int result = -1;
		if (conn == null)
			return -1;
		try {
			//updated by hans, Remove the duplicate usage of the connection and preparedstatement, for potential memory leak.
//			conn.prepareStatement(sql);
			// updated end
			ResultSet rs = stmt.executeQuery(sql);
			while(rs.next())
				result = rs.getInt(1); 
			
			rs.close();
			
		} catch (SQLException e) {
			new ETLException(e).printStackTrace();
			log.error("执行语句: " + sql);
			log.error("错误原因: " + e.getMessage().toString());
			return -1;
		}
		return result;
	}
	
	public ResultSet execQuery(String querySql) {
		log.debug(querySql);
		if (conn == null) {
			return null;
		}
		
		try {
			if(stmt ==null)
				stmt = conn.createStatement();
			//updated by hans, Remove the duplicate usage of the connection and preparedstatement, for potential memory leak.
//			conn.prepareStatement(querySql);
			//updated end
			rs = stmt.executeQuery(querySql);
			if (rs != null)
				return rs;
			else
				return null;
		} catch (Exception e) {
			new ETLException(e).printStackTrace();
			log.error("查询语句: " + querySql);
			log.error("错误原因: " + e.getMessage().toString());
			return null;
		}
	}

	public String exec_str(String sql) {
		log.debug(sql);
		String result = "";
		if (conn == null)
			return result;
		try {
			if(stmt ==null)
				stmt = conn.createStatement();
			//updated by hans, Remove the duplicate usage of the connection and preparedstatement, for potential memory leak.
//			conn.prepareStatement(sql);
			// updated end
			stmt.executeUpdate(sql);
		} catch (SQLException e) {
			new ETLException(e).printStackTrace();
			log.error("执行语句: " + sql);
			log.error("错误原因: " + e.getMessage().toString());
			result = e.getMessage().toString();
		}
		return result;
	}

	public int exec(String sql) {
		log.debug(sql);
		int result = -1;
		if (conn == null)
			return -1;
		try {
			// updated by hans, Remove the duplicate usage of the connection and preparedstatement, for potential memory leak.
//			conn.prepareStatement(sql);
			// updated end
			result = stmt.executeUpdate(sql);
		} catch (SQLException e) {
			new ETLException(e).printStackTrace();
			log.error("执行语句: " + sql);
			log.error("错误原因: " + e.getMessage().toString());
			return -1;
		}
		return result;
	}
	
	// added by hans, for Escape the ' in the card name for ETL server
	public String exec_str(String sql,String[] args) {
		log.debug(sql);
		log.debug(args.toString());
		String result = "";
		if (conn == null)
			return result;
		PreparedStatement pstmt=null;
		try {
			pstmt= conn.prepareStatement(sql);
			for(int i=0;i<args.length;i++){
				pstmt.setString(i+1, args[i]);
			}
			pstmt.executeUpdate();
		} catch (SQLException e) {
			log.error("执行语句: " + sql);
			log.error("错误原因: " + e.getMessage().toString());
			result = e.getMessage().toString();
			new ETLException(e).printStackTrace();
		}finally{
			if(pstmt!=null){
				try {
					pstmt.close();
				} catch (SQLException e) {
					new ETLException(e).printStackTrace();
				}
			}
		}
		return result;
	}
	public int exec(String sql,String[] args) {
		log.debug(sql);
		log.debug(args.toString());
		int result = -1;
		if (conn == null)
			return -1;
		PreparedStatement pstmt=null;
		try {
			pstmt= conn.prepareStatement(sql);
			for(int i=0;i<args.length;i++){
				pstmt.setString(i+1, args[i]);
			}
			result = pstmt.executeUpdate();
		} catch (SQLException e) {
			log.error("执行语句: " + sql);
			log.error("错误原因: " + e.getMessage().toString());
			new ETLException(e).printStackTrace();
			return -1;
		}finally{
			if(pstmt!=null){
				try {
					pstmt.close();
				} catch (SQLException e) {
					new ETLException(e).printStackTrace();
				}
			}
		}
		return result;
	}	// added end
	
	public void freeConn() {
		try {
			if (stmt != null)
				stmt.close();
			if (conn != null) {
				conn.close();
			}
		} catch (Exception e) {
			new ETLException(e).printStackTrace();
		} finally {
			try {
				if (stmt != null)
					stmt.close();
				if (conn != null)
					conn.close();
			} catch (Exception ex1) {
				new ETLException(ex1).printStackTrace();
			}
		}
	}
}
