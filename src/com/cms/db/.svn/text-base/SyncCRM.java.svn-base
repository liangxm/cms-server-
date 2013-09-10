package com.cms.db;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.cms.exception.ETLException;
import com.cms.util.CMSServerConfig;

public class SyncCRM {

	static Logger log = Logger.getLogger("SyncCRM");
	static {
		PropertyConfigurator.configure("./conf/log4j.properties");

	}

	public Properties properties = CMSServerConfig.getProperties();

	public DBPubFuncs dbpub = new DBPubFuncs();
	public DBPubFuncs dbpub2 = new DBPubFuncs();
	public StringBuffer bufSQL = new StringBuffer();

	public static void main(String[] args) {

		SyncCRM sync = new SyncCRM();
		sync.SyncToMember();
		sync.SyncToSales();

		sync.free();
	}

	public void free() {
		dbpub.freeConn();
		dbpub2.freeConn();
	}

	public void SyncToMember() {

		SimpleDateFormat sf2 = new SimpleDateFormat("yyyyMM");
		String yearMonth = sf2.format(new Date());
		ResultSet rs2 = null;

		try {
			rs2 = dbpub2
					.execQuery("SELECT CARD_NUM, sum( T.C ) , MAX( T.C ) , t.Location "
							+ " FROM (SELECT CARD_NUM, COUNT( CARD_NUM ) AS c, t.Location "
							+ " FROM "+properties.getProperty("SRC_TB")
							+ " LEFT JOIN sn_location_map t ON POS_ID = t.SN"
							+ " WHERE SUBSTR( CREATE_DATE, 1, 6 ) = '"
							+ yearMonth
							+ "' "
							+ " GROUP BY CARD_NUM, POS_ID)T GROUP BY T.CARD_NUM");
			String error = "";
			while (rs2.next()) {
				bufSQL.delete(0, bufSQL.length());

				String CARD_NUM = rs2.getString(1);
				String TotalVisits = rs2.getString(2);
				String Location = rs2.getString(4);

				bufSQL.append("\n update " + properties.getProperty("CRM_MEMBER")
						+ " " + "set " + properties.getProperty("CRM_VISIT")
						+ "='" + TotalVisits + "', "

						+ properties.getProperty("CRM_LOC_MOST") + "='"
						+ Location + "' "

						+ "where card_number = '" + CARD_NUM + "'");

				error = dbpub.exec_str(bufSQL.toString());
				// System.out.println(bufSQL.toString() + error);
				System.out.println("Sync to member OK");
				if (error.length() > 0) {
					log.error("Have error:" + error);
				}

			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			new ETLException(e).printStackTrace();
		}
	}

	public void SyncToSales() {

		SimpleDateFormat sf2 = new SimpleDateFormat("yyyyMM");
		String yearMonth = sf2.format(new Date());
		ResultSet rs2 = null;

		try {
			rs2 = dbpub2.execQuery("SELECT * FROM ("
					+ " SELECT CARD_NUM, COUNT( CARD_NUM ) AS C, t.Location"
					+ " FROM tb_settlement_srfc_weekly tt"
					+ " LEFT JOIN sn_location_map t ON POS_ID = t.SN"
					+ " WHERE SUBSTR( CREATE_DATE, 1, 6 ) = '" + yearMonth + "'"
					+ " GROUP BY CARD_NUM, POS_ID" + "ORDER BY C DESC )tt"
					+ " WHERE tt.C >= ( " + "SELECT COUNT( CARD_NUM ) AS cc"
					+ " FROM tb_settlement_srfc_weekly"
					+ " WHERE SUBSTR( CREATE_DATE, 1, 6 ) = '" + yearMonth + "'"
					+ " GROUP BY CARD_NUM, POS_ID"
					+ " ORDER BY cc DESC LIMIT 10 , 1 )");
			String error = "";
			while (rs2.next()) {
				bufSQL.delete(0, bufSQL.length());

				String CARD_NUM = rs2.getString(1);
				String TotalVisits = rs2.getString(2);
				String Location = rs2.getString(4);

				bufSQL.append("\n update " + properties.getProperty("CRM_SALES")
						+ " " + "set " + properties.getProperty("CRM_VISIT")
						+ "='" + TotalVisits + "', "

						+ properties.getProperty("CRM_LOC_MOST") + "='"
						+ Location + "' "

						+ "where card_number = '" + CARD_NUM + "'");

				error = dbpub.exec_str(bufSQL.toString());
				// System.out.println(bufSQL.toString() + error);
				System.out.println("OK" + error);
				if (error.length() > 0) {
					log.error("Have error");
				}

			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			new ETLException(e).printStackTrace();
		}
	}

}
