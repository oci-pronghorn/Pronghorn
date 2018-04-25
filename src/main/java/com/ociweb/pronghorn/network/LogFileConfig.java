package com.ociweb.pronghorn.network;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogFileConfig {

    private static final long DEFAULT_SIZE = 1L<<28;//256M
    private static final int DEFAULT_COUNT = 20;
	private static Logger logger = LoggerFactory.getLogger(LogFileConfig.class);
    
	private final String baseFileName;
	private final long maxFileSize;
	private final int countOfFiles;
		
	public LogFileConfig() {
		this(defaultPath());
	}

	public static String defaultPath() {
		String home = System.getenv().get("HOME");
		if (null==home) {
			return tempPath();
		} else {
			try {
				return File.createTempFile("green", "", new File(home)).getAbsolutePath();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}
	
	public static String tempPath() {
		String basePath = "";
		try {
			basePath = File.createTempFile("green", "").getAbsolutePath();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return basePath;
	}
	
	public LogFileConfig(String baseFileName) {
		this(baseFileName, DEFAULT_COUNT, DEFAULT_SIZE);
	}
	
	public LogFileConfig(String baseFileName, 
			             int countOfFiles, 
			             long maxFileSize) {
		this.baseFileName = baseFileName;
		this.maxFileSize = maxFileSize;		
		this.countOfFiles = countOfFiles;
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

	
}
