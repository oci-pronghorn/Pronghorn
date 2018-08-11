package com.ociweb.pronghorn.network;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogFileConfig {

    public static final long DEFAULT_SIZE = 1L<<28;//256M
    public static final int DEFAULT_COUNT = 20;
	private static Logger logger = LoggerFactory.getLogger(LogFileConfig.class);
    
	private final String baseFileName;
	private final long maxFileSize;
	private final int countOfFiles;
	private boolean logResponse;
		
	public static String defaultPath() {
		String home = System.getenv().get("HOME");
		if (null==home) {
			return tempPath();
		} else {
			try {
				File createTempFile = File.createTempFile("green", "", new File(home));
				String absolutePath = createTempFile.getAbsolutePath();
				createTempFile.delete();
				return absolutePath;
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}
	
	public static String tempPath() {

		try {
			File createTempFile = File.createTempFile("green", "");
			String absolutePath = createTempFile.getAbsolutePath();
			createTempFile.delete();
			return absolutePath;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public LogFileConfig(String baseFileName, 
			             int countOfFiles, 
			             long maxFileSize,
			             boolean logResponse) {
		
		this.baseFileName = baseFileName;
		this.maxFileSize = maxFileSize;		
		this.countOfFiles = countOfFiles;
		this.logResponse = logResponse;
		logger.info("base logging file location:\n{}",baseFileName);
		
	}
	
	public String base() {
		return baseFileName;
	}
	
	public int countOfFiles() {	
		return countOfFiles;
	}
	
	public long maxFileSize() {
		return maxFileSize;
	}

	public boolean logResponses() {
		return logResponse;
	}
	
}
