package com.cms.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;

import com.cms.exception.ETLException;

public class FileUtilities {
	
	public void newFile(String filePathAndName, String fileContent) {

		try {
			String filePath = filePathAndName;
			filePath = filePath.toString();
			File myFilePath = new File(filePath);
			if (!myFilePath.exists()) {
				myFilePath.createNewFile();
			}
			FileWriter resultFile = new FileWriter(myFilePath);
			PrintWriter myFile = new PrintWriter(resultFile);
			String strContent = fileContent;
			myFile.println(strContent);
			resultFile.close();

		} catch (Exception e) {
			System.out.println("新建目录操作出错 ");
			new ETLException(e).printStackTrace();

		}

	}
	
	public void copy(File src, File dst) {
		try {
			InputStream in = null;
			OutputStream out = null;
			try {
				in = new BufferedInputStream(new FileInputStream(src));
				out = new BufferedOutputStream(new FileOutputStream(dst));
				byte[] buffer = new byte[in.available()];
				while (in.read(buffer) > 0) {
					out.write(buffer);
				}
			} finally {
				if (null != in) {
					in.close();
				}
				if (null != out) {
					out.close();
				}
			}
		} catch (Exception e) {
			new ETLException(e).printStackTrace();
		}
	}

	public ArrayList<String> getAllFiles(String path) {
		ArrayList<String>  fileList = new ArrayList<String>();
		File file = new File(path);
		// get the folder list
		File[] array = file.listFiles();
		int L = 0;
		if(array!=null)
			L = array.length;
			System.out.println(L);
			
		
		for (int i = 0; i < L; i++) {
			if (array[i].isFile()) {
				// only take file name
				// System.out.println("^^^^^" + array[i].getName());
				// take file path and name
				// System.out.println("#####" + array[i]);
				// take file path and name
				// System.out.println("*****" + array[i].getPath());
				System.out.print(array[i].getPath());
				String[] filePath = array[i].getPath().split("\\\\");
				fileList.add(filePath[filePath.length - 1]);
			}
		}
		return fileList;
	}
}
