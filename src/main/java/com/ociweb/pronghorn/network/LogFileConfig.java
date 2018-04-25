package com.ociweb.pronghorn.network;

import java.io.File;
import java.io.IOException;

public class LogFileConfig {

    private static final long DEFAULT_SIZE = 1L<<28;//256M
    private static final int DEFAULT_COUNT = 20;
	
	private final String baseFileName;
	private final long maxFileSize;
	private final int countOfFiles;
		
	public LogFileConfig() {
		this(defaultPath());
	}

	private static String defaultPath() {
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
