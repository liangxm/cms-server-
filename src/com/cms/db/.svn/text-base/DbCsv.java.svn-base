package com.cms.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import com.cms.exception.ETLException;
import com.cms.util.CMSServerConfig;

public class DbCsv {
	private Connection conn;
	private Statement stmt;
	private static ResultSet rs;
	Properties props = new Properties();
	public static void main(String[] args) {
		DbCsv db = new DbCsv();
		rs = db.execQuery("select * from EB101860013400000420100928162488");
	
		try {
			while (rs.next()) {
			System.out.println(rs.getString(1));

			}
		} catch (SQLException e) {
			new ETLException(e).printStackTrace();
		}
	}
	public DbCsv() {
		createConnection();
	}

	private void createConnection() {
		conn = null;
		stmt = null;
		try {
			Properties properties = CMSServerConfig.getProperties();
			String path = properties.getProperty("data.home");
			Class.forName("org.relique.jdbc.csv.CsvDriver").newInstance();
			props.put("charset","GB2312");
			/*
			 * 灰常重要 ！suppressHeaders为true，则CVS无列名，默认值是false。
			 * 第一行就是数据;否则，CSV第一行是表头，数据从第二行开始。
			 */
			props.put("suppressHeaders","true");
			conn = DriverManager.getConnection("jdbc:relique:csv:"+path,props);
			stmt = conn.createStatement();
		} catch (Exception e) {
			new ETLException(e).printStackTrace();
		}
	}

	public ResultSet execQuery(String querySql) {
		if (conn == null) {
			return null;
		}
		try {
			rs = stmt.executeQuery(querySql);
			if (rs != null)
				return rs;
			else
				return null;
		} catch (Exception e) {
			new ETLException(e).printStackTrace();
			return null;
		}
	}

	public int exec(String sql) {
		int result = -1;
		if (conn == null)
			return -1;
		try {
			conn.prepareStatement(sql);
			result = stmt.executeUpdate(sql);
		} catch (SQLException e) {
			new ETLException(e).printStackTrace();
			return -1;
		}
		return result;
	}

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
