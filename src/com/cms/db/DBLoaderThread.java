package com.cms.db;

import java.io.File;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.cms.exception.ETLException;
import com.cms.util.CMSServerConfig;
import com.cms.util.FileUtilities;

public class DBLoaderThread implements Runnable {
	SimpleDateFormat sfTrxID = new SimpleDateFormat("yyyyMMddHHmmss");
	FileUtilities fu = new FileUtilities();

	Properties properties = CMSServerConfig.getProperties();
	static Logger log = Logger.getLogger("DBServer");

	/*loading configuration file*/
	static { 
		PropertyConfigurator.configure(CMSServerConfig.getConfigPathURL());
	}

	public boolean runnable = true;
	public String FilepathFlag;
	File br;
	int refreshTime;
	
	// csv table
	ResultSet rs = null;
	
	// system setting
	ResultSet rs2 = null;
	
	// cms table
	ResultSet rs3 = null;
	
	// cms table added by lxm for redmine #710
	ResultSet rs4 = null;
	
	//db_file_status file status
	ResultSet statusRs = null;
	private String filepath = "";

	public DBLoaderThread(String filepath, String reftime) {
		this.filepath = filepath;
		br = new File(filepath);
		refreshTime = Integer.valueOf(reftime) * 60 * 1000; // Define
	}

	private boolean isValidLogCompleteFileName(String fileWholeName){
		if(fileWholeName.startsWith("Error_")){
			return false;
		}
		if(fileWholeName.length()>"EB1018600134000002".length() && fileWholeName.endsWith(".csv")){
			return true;
		}
		return false;
	}
	public void run() {
		log.info("Database Server Start!");
		try{
		while (runnable) {
			try {
				System.out.println(filepath);
				File[] fs_s = br.listFiles();
				if (fs_s != null){
					
					/*judge this file is or not sending use file size*/
					long fileSize = 0;
					for (File f : fs_s) {
						if (f.isDirectory() || !f.exists()) {
							/**
							 * if this file is Directory or doesn't exists
							 * nothing do
							 */
						} else {
							/**
							 * if this file exists
							 * start operation file
							 */
							String statusFileName = f.getName();
							String  FileName = statusFileName.substring(0,f.getName().indexOf("."));
							log.info("New File: "+ FileName+" ["+statusFileName+"]");
								
							/**
							 * confirm file is or not receive done.
							 * if not sleep a moment
							 */
							DBPubFuncs dbpub = new DBPubFuncs();
							
							/*get file status*/
							String fileStatus = "";
							statusRs = dbpub.execQuery("select status from tb_file_status where fileName='" + statusFileName + "'");
							if(statusRs.next()){
								fileStatus = statusRs.getString("status");
							}else{
								/**
								 * if here than means 
								 * this file is a file by uploading manually
								 */
								
								/*get this file time last modified*/
								long lLastModified=f.lastModified();
								long msNoUpdating=new Date().getTime() - lLastModified;
								
								/**
								 * if file has kept 2 minutes than operation
								 * else skip this file
								 */
								if(isValidLogCompleteFileName(statusFileName) 
										&& msNoUpdating >= 120*1000){
									
									/**
									 * import database as a normal correct file
									 * set the initial time as null (indicate this is a manually uploading file.
									 */
									fileStatus="02";
									dbpub.exec_str("insert into tb_file_status values('" + statusFileName
											+ "','"+fileStatus+"',null,'" + 
											new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + "')");
									}else{
										
										/**
										 * assume this file is invalid or 
										 * it's still transferring, skip this file.
										 */
										log.info("skip file ["+statusFileName+"] lastmodified["+new Date(lLastModified)+"]");
										continue;
									}
								}
								try{
									statusRs.close();
								}catch(SQLException se){
									new ETLException(se).printStackTrace();
									log.fatal("unexpect: close statusRs after query tb_file_status failure", se);
								}
								log.info("File Status:" + fileStatus);
								boolean isSending = true;
								fileSize = f.length();
								
								/**
								 * if file status equals "01" means this file is sending.
								 * sleeping 2 minutes
								 * */
								if("01".equals(fileStatus)){
									Thread.sleep(120*1000);
								}
								
								/**
								 * if file size more than 0 and less than file's length
								 * means this file is still sending
								 */
								if(fileSize > 0 && fileSize < f.length() ){
									isSending = true;
								}else{
									isSending = false;
								}
								log.info("isSending========" + isSending);
								if (loadToSQL(f.getName().substring(0,
										f.getName().indexOf(".")),statusFileName,isSending,dbpub,fileStatus)) {
									SimpleDateFormat sf = new SimpleDateFormat(
											"yyyy_MM_dd");
									if (FilepathFlag == null)
										FilepathFlag = "Others";
									String path = filepath
											+ FilepathFlag + "/OldTrx_"
											+ sf.format(new Date());
									File oldPath = new File(path);
									if (!oldPath.exists()) {
										oldPath.mkdirs();
									}
									
									/**
									 * Load to DB successful!
									 * move this file to special custom folder
									 * and delete this file at current folder
									 * */
									File oldOne = new File(path + "/"
											+ f.getName());
									fu.copy(f, oldOne);
									f.delete();
								}
							}
						}
					}else{
						break;
					}
			} catch (NullPointerException e1) {
				log.fatal(e1);
				//TODO//new ETLException(ex).printStackTrace();
			} catch (SQLException e2){
				new ETLException(e2).printStackTrace();
			}finally{
				log.info("Wait new file....After "
						+ (refreshTime / (60 * 1000)) + " minutes refresh.");
				synchronized (this) {
					this.wait(refreshTime);
				}
			}
		}
		}catch(InterruptedException ie){
			new ETLException(ie).printStackTrace();
			log.fatal("exception when waiting: "+ie);
		}
		runnable=false;
		log.info("Database Server Shutdown!");
	}

	public boolean loadToSQL(String FileName,String statusFileName,boolean isSending,DBPubFuncs dbpub,String fileStatus) {

		StringBuffer bufSQL = new StringBuffer();
		SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String POS_ID = "";

		int fileNameLen = "EB1018600134000002".length();

		DbCsv db = new DbCsv();
		
		DBPubFuncs dbpub2 = new DBPubFuncs();

		int csvcount = 0;
		int counterror = 0;

		boolean isError = false;
		
		//If is error file
		//File name size > Error_   or  File name size < fileNameLen
		if (FileName.length()<=fileNameLen ) {
			FilepathFlag = "ERROR/";
			log.info("It's error file.");
			log.info("Move to the Error folder.");
			String result2 = dbpub.exec_str("update tb_file_status set status='03',updateTime='" + sf.format(new Date()) + "' where fileName='" + statusFileName + "' and status='01'");
			if(result2.length() > 0){
				log.info("update db_file_status filename=" + statusFileName + "error");
				return false;
			}
			log.info("update db_file_status filename=" + statusFileName + ";status=03");
			return true;
		}else{
			//If is normal file
			POS_ID = FileName.substring(0, fileNameLen);
		}
		
		try {
			
			if("01".equals(fileStatus)){
				if(isSending){
					log.info("fileName:" + statusFileName + " is sending");
					return false;
				}else{
					log.info("fileName:" + statusFileName + " is error file");
					isError = true;
				}
			}
			
			/**
			 * get special customer table info by POS_ID 
			 * */
			String startDate = sf.format(new Date());
			rs2 = dbpub
					.execQuery("SELECT s.Tablename,s.TableColumn,s.ProjectName,s.FilepathFlag,s1.Location,s.CRM_TB,"
							+ "s.CRM_CARD_BALANCE,s.CRM_CARD_POINTS,s.CRM_FILTER_PARM"
							+ " FROM cms.sys_setting s"
							+ " left join cms.sn_location_map s1 on s.ProjectName = s1.ProjectName where "
							+ "  s1.SN ='" + POS_ID + "'");

			String tableName = "";
			String tableFields = "";
			String CRMTableName = "";
			String ProjsectName = "";
			String CRM_CARD_BALANCE = "";
			String CRM_CARD_POINTS = "";
			String CRM_FILTER = "";
			String   CRMNewCustTableName = "";

			while (rs2.next()) {

				tableName = rs2.getString("Tablename");
				tableFields = rs2.getString("TableColumn");
				FilepathFlag = rs2.getString("FilepathFlag");
				CRMTableName = rs2.getString("CRM_TB");
				CRM_CARD_BALANCE = rs2.getString("CRM_CARD_BALANCE");
				CRM_CARD_POINTS = rs2.getString("CRM_CARD_POINTS");
				ProjsectName = rs2.getString("ProjectName");

				CRM_FILTER= rs2.getString("CRM_FILTER_PARM");
			}
			
			/**
			 * Determine whether the error file.
			 * if it is then move to ERROR/ProjectName directory.
			 */
			if (isError) {
				FilepathFlag = "ERROR/"+ProjsectName;
				log.info("It's error file.");
				log.info("Move to the Error folder.");
				String result2 = dbpub.exec_str("update tb_file_status set status='03',updateTime='" + sf.format(new Date()) + "' where fileName='" + statusFileName + "' and status='01'" );
				if(result2.length() > 0){
					log.info("update db_file_status filename=" + statusFileName + "error");
					return false;
				}
				log.info("update db_file_status filename=" + statusFileName + ";status=03");
				return true;
			}
			
			/**
			 * Determine whether the QA file.
			 * if it is than move to QA/ProjectName directory.
			 */
			if (tableName.length() == 0 || tableName.equalsIgnoreCase("QA")) 
			{
				FilepathFlag = "QA";
				log.info("It's QA file.");
				log.info("Move to the QA folder.");
				String result2 = dbpub.exec_str("update tb_file_status set status='03',updateTime='" + sf.format(new Date()) + "' where fileName='" + statusFileName + "'");
				if(result2.length() > 0){
					log.info("update db_file_status filename=" + statusFileName + "error");
					return false;
				}
				log.info("update db_file_status filename=" + statusFileName + ";status=03");
				return true;
			}

			if(CRMTableName.length() >0){
				CRMNewCustTableName = CRMTableName.substring(0,CRMTableName.lastIndexOf("_cstm"));
			}

			/*get csv file all fields*/
			rs = db.execQuery("select * from " + FileName);
			Set<String> numberSet = new HashSet<String>();
			numberSet.clear();
			try{
			while (rs.next()) {
				numberSet.add(rs.getString(1));
				bufSQL.delete(0, bufSQL.length());

				if(CRMTableName.length() >0){
					
					/*get transaction code*/
					String trxCode = rs.getString(10);
					
					/**
					 * Determine whether the initialization transaction.
					 * if it is goto logic inside
					 * */
					if(trxCode.equalsIgnoreCase("I")||trxCode.equalsIgnoreCase("IP")||trxCode.equalsIgnoreCase("IC")){
						
						/* filter logic by filter field*/
						String[] type = CRM_FILTER.split(",");
						boolean filtered = false;
						for (String string : type) {
							if(string.trim().equalsIgnoreCase(rs.getString(2)) ){
								filtered= true;
								System.out.println("filtered:"+string);
							}
						}

						/**
						 * if needn't filter than goto logic inside
						 * */
						if(!filtered){
							log.info("sql======" + "Select count(*)  from "+CRMTableName +" a left join " + CRMNewCustTableName + " b on a.id_c=b.id where  a.id_c = "+ rs.getString(1) + " or a.card_number_c=" + rs.getString(1));
							int rows = dbpub2.exec_getRowNum("Select count(*)  from "+CRMTableName +" a left join " + CRMNewCustTableName + " b on a.id_c=b.id where  a.id_c = "+ rs.getString(1) + " or a.card_number_c=" + rs.getString(1));
							log.info("rows=========" + rows);
							System.out.println("filtered:"+rows);
							
							/**
							 * if contacts_cstm and contacts table haven't info of this card number
							 * insert data to contacts and contacts_cstm table
							 * */
							if(rows==0){
								dbpub2.exec("INSERT INTO  "+CRMTableName +"  (id_c,card_id_name_c,card_money_balance_c,card_number_c,member_sports_c,created_at_c,member_type_c ) " +
										" values ('"+rs.getString(1) +"','"+rs.getString(14).replace("'", "''") +"',0,'"+rs.getString(1) +"','"+rs.getString(21) +"','"+rs.getString(20) +"','"+rs.getString(2) +"')");
								dbpub2.exec("INSERT INTO  "+CRMNewCustTableName   +"  (id,last_name,date_entered,date_modified,deleted ) values ('"+rs.getString(1) +"','"+rs.getString(14).replace("'","''") +"',now(),now(),0)");
							}
						}
					}
					
					/**
					 * store lastest trx date to CRM table
					 */
					String strTrxID=rs.getString(9);
					Date dLastTrx=new Date();
					try {
						dLastTrx = sfTrxID.parse(strTrxID);
					} catch (ParseException e) {
						new ETLException(e).printStackTrace();
						log.error("exception "+e+", Invalid trxid format: "+strTrxID +", will use current datetime as default.");
					}
					String strsql="update "+CRMTableName+" set last_transaction_date_c = '"+ sf.format(dLastTrx) +"' where card_number_c='"+rs.getString(1)+"' "
					+ " and (last_transaction_date_c is null or last_transaction_date_c < '"+sf.format(dLastTrx)+"')";
					String error = dbpub.exec_str(strsql);
					if (error.length() > 0) {
						String strAlter = "ALTER TABLE "+CRMTableName+" ADD last_transaction_date_c DATETIME";
						log.error("Assume the last_transaction_date_c field is missing: "+strAlter);
						dbpub.exec_str(strAlter);
						log.info("execute the update statement again.");
						error = dbpub.exec_str(strsql);
					}
					
					/**
					 * set the active when B E UB
					 * */
					if(trxCode.equalsIgnoreCase("B")||trxCode.equalsIgnoreCase("E")||trxCode.equalsIgnoreCase("UB")){
						String strsqlActive="update "+CRMTableName+" set active_c = "+ (trxCode.equalsIgnoreCase("UB")?1:0) +" where card_number_c='"+rs.getString(1)+"'";
						error= dbpub.exec_str(strsqlActive);
					}
				}
				
				/**
				 * load data to tb_rpd_? table.
				 */
				int tableFieldsCount = tableFields.split(",").length;
				String args[] = new String[tableFieldsCount];
				StringBuffer sbValuePosition=new StringBuffer();
				for (int i = 1; i <= args.length; i++) {
					if (rs.getString(i) != null && rs.getString(i).toString().trim().length() > 0){
						if(i==args.length){
							args[i-1]=rs.getString(i).replace(" ", "");
						}else{
							args[i-1]=rs.getString(i);
						}
					}else{
						args[i-1]=null;
					}
					sbValuePosition.append("?,");
				}
				String strSql="insert into " + tableName 
						+ " (POS_ID," + tableFields + ",Version) "
						+ " values( '" + POS_ID + "', " + sbValuePosition.toString()
						+ "'" + startDate + "')";

				String error = "";

				error = dbpub.exec_str(strSql,args);
				if (error.length() > 0) {
					counterror++;
				}
				csvcount++;
			}
			}
			catch(SQLException e2){
				counterror++;
				new ETLException(e2).printStackTrace();
			}
			
			/**
			 * Load to CRM System
			 * columns update to CRM
			 */
			int cardCount = 0;
			if (CRMTableName.length() != 0) {
				Properties properties = CMSServerConfig.getProperties();
				String LOC_CARD_NUM = properties.getProperty("LOC_CARD_NUM");
				String LOC_END_BALANCE = properties
						.getProperty("LOC_END_BALANCE");
				String LOC_END_POINTS = properties
						.getProperty("LOC_END_POINTS");
				String LOC_CARD_ID = properties.getProperty("LOC_CARD_ID");
				String LOC_CARD_TYPE = properties.getProperty("LOC_CARD_TYPE");
				String LOC_TEAM_NAME = properties.getProperty("LOC_TEAM_NAME");
				
				String LOC_REF_VALUES = properties.getProperty("LOC_REF_VALUES");
				
				numberSet.remove("9999999999999999");//MS
				for(String number:numberSet){
					String strSql="SELECT " + LOC_CARD_NUM
							+ "," + LOC_END_BALANCE + "," + LOC_END_POINTS
							+ "," + LOC_CARD_ID + "," + LOC_CARD_TYPE
							+ "," + LOC_TEAM_NAME + "," + LOC_REF_VALUES
							+ " FROM " + tableName + " "
							+ " WHERE TRX_ACTION_CD <> 'B' AND CARD_NUM ='"+number+"'"
							+ " ORDER BY TRX_ID DESC"
							+ " LIMIT 1";
					log.info("rs3 sql: "+strSql);
					
					/*get data from tb_rpd_? table for update contacts_cstm*/
					rs3 = dbpub2
							.execQuery(strSql);
					log.info("rs3 sql return");
					
					if(rs3.next() &&  runnable) {
						bufSQL.delete(0, bufSQL.length());
						
						String cardBalance = "";
						if (rs3.getString(LOC_END_BALANCE) != null
								&& rs3.getString(LOC_END_BALANCE).trim().length() > 0)
							cardBalance = rs3.getString(LOC_END_BALANCE);
						else
							cardBalance = "0";
						
						String cardPoints = "";
						if (rs3.getString(LOC_END_POINTS) != null
								&& rs3.getString(LOC_END_POINTS).trim().length() > 0)
							cardPoints = rs3.getString(LOC_END_POINTS);
						else
							cardPoints = "0";
						
						String cardID = "";
						if (rs3.getString(LOC_CARD_ID) != null
								&& rs3.getString(LOC_CARD_ID).trim().length() > 0)
							cardID = rs3.getString(LOC_CARD_ID);
						else
							cardID = "0";
						String cardType = "";
						if (rs3.getString(LOC_CARD_TYPE) != null
								&& rs3.getString(LOC_CARD_TYPE).trim().length() > 0)
							cardType = rs3.getString(LOC_CARD_TYPE);
						else
							cardType = "None";
						
						String teamName = "";
						if (rs3.getString(LOC_TEAM_NAME) != null
								&& rs3.getString(LOC_TEAM_NAME).trim().length() > 0)
							teamName = rs3.getString(LOC_TEAM_NAME);
						else
							teamName = "None";
						
						String cardNumber = "";
						if (rs3.getString(LOC_CARD_NUM) != null
								&& rs3.getString(LOC_CARD_NUM).trim().length() > 0)
							cardNumber = rs3.getString(LOC_CARD_NUM);
						else
							cardNumber = "unknow";
						
						String reference_value = "";
						if(rs3.getString(LOC_REF_VALUES)!=null 
								&& rs3.getString(LOC_REF_VALUES).trim().length() > 0){
							reference_value = rs3.getString(LOC_REF_VALUES);
						}else{
							reference_value = "unknow";
						}
						
						String updated_on = " updated_on ";
						String card_number = " card_id ";
						if (CRMTableName.substring(CRMTableName.length() - 5,
								CRMTableName.length()).equals("_cstm")) {
							updated_on = " updated_on_c ";
							card_number = " card_number_c ";
						}
						
						/**
						 * update crm table 
						 * just update card_id when current transaction is SRFC Card
						 */
						if(ProjsectName.equals("SRFC") && POS_ID.equals("PC0000000000000098")){
							bufSQL.append("\n update " + CRMTableName + " " + "set "
									+ CRM_CARD_BALANCE + "='" + cardBalance + "', "
									+ CRM_CARD_POINTS + "='" + cardPoints + "', "
									+ "card_id_name_c='" + cardID.replace("'", "''") + "',"
									+ "member_type_c='" + cardType + "',"
									+ "member_sports_c='" + teamName + "',"
									+ "cms_expiry_date_c='" + reference_value + "',"
									+ updated_on + " = '" + startDate + "' " + "where "
									+ card_number + " = '" + cardNumber + "'");
						}else{
							bufSQL.append("\n update " + CRMTableName + " " + "set "
									+ CRM_CARD_BALANCE + "='" + cardBalance + "', "
									+ CRM_CARD_POINTS + "='" + cardPoints + "', "
									+ "card_id_name_c='" + cardID.replace("'", "''") + "',"
									+ "member_type_c='" + cardType + "',"
									+ "member_sports_c='" + teamName + "',"
									+ updated_on + " = '" + startDate + "' " + "where "
									+ card_number + " = '" + cardNumber + "'");
						}
						
						
						String error = "";
						log.info("update "+cardNumber);
						log.debug("SQL--------------->:"+bufSQL.toString());
						error = dbpub.exec_str(bufSQL.toString());
						log.info("res: "+error);
						
						if (error.length() > 0) {
							log.info("Meet error:"+error);
							counterror++;
						}
						cardCount++;
					}
				}
			}
			if(runnable){
			if (counterror == 0) {
				log.info("Insert " + csvcount + " transactions to the "
						+ ProjsectName + " DB.");
				log.info("Synchronized " + cardCount + " card values to the "
						+ ProjsectName + " CRM System.");
				
				/**
				 * set file status equals "04" when insert to database successful
				 */
				String strSql="update tb_file_status set status='04',updateTime='" + sf.format(new Date()) + "' where fileName='" + statusFileName + "'";
				log.info("update file status sql: "+strSql);
				String result2 = dbpub.exec_str(strSql);
				if(result2.length() > 0){
					log.info("update db_file_status filename=" + statusFileName + "error");
					return false;
				}
				log.info("update db_file_status filename=" + statusFileName + ";status=04");
				return true;
			} else {
				/**
				 * set file status equals "03" when insert to database failure
				 */
				String strSql="update tb_file_status set status='03',updateTime='" + sf.format(new Date()) + "' where fileName='" + statusFileName + "'";
				log.info("update file status sql: "+strSql);
				String result2 = dbpub.exec_str(strSql);
				if(result2.length() > 0){
					log.info("update db_file_status filename=" + statusFileName + "error");
					return false;
				}
				FilepathFlag = "ERROR/"+ProjsectName;
				log.info("Have some error~.");
				log.info("Move to the ERROR folder.");
				log.info("update db_file_status filename=" + statusFileName + ";status=03");
				return true;
			}
			}else{
				log.info("DBProcess has been terminated by tomcat!!");
				return false;
			}
		} catch (SQLException e) {
			new ETLException(e).printStackTrace();
			return false;
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (rs2 != null)
					rs2.close();
				if (rs3 != null)
					rs3.close();
				if(statusRs != null){
					statusRs.close();
				}
			} catch (SQLException e) {
				new ETLException(e).printStackTrace();
			}
			db.freeConn();
			dbpub.freeConn();
			dbpub2.freeConn();
		}
	}
}