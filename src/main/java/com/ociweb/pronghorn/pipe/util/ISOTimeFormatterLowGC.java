package com.ociweb.pronghorn.pipe.util;

import java.time.Instant;
import java.time.format.DateTimeFormatter;


import com.ociweb.pronghorn.util.AppendableByteWriter;

public class ISOTimeFormatterLowGC {
	
	//TODO: data capture in place
	//TODO: switch on with file name location
	//TODO: file rotation - use console first before going to file.
//Must be done by tomorrow!!!
	
	//TODO: limited GC date formatter
	
	////////do not modify these constants
	private static final String YYYY_MM_DD_HH_MM_SS_SSS_Z = "yyyy-MM-dd HH:mm:ss.SSS z";
	private static final int SECONDS_OFFSET = 17; //where the seconds begin
	private static final int SECONDS_LENGTH = 6; //where the seconds begin
	
	private static final int SECONDS_PLACES = -3; //sub second places of accuracy (always negative)
	////////////////////////////////////
	
	private final DateTimeFormatter formatter;
	private long validRange = 0;
	
	public ISOTimeFormatterLowGC() {
		formatter = DateTimeFormatter.ofPattern(YYYY_MM_DD_HH_MM_SS_SSS_Z);
	
	}
		
	public void write(long time, AppendableByteWriter writer) {
		
		long localMinute = time/60_000L;
		
		//TODO: only do when out of range
		//TOOD: write to a temp pipe instead of diret to writer.
		//if (isOutOfRange(time)) {
			//build new template
			
			
		    //this is so we know that we are in the same minute next time
		    validRange = localMinute; 
		    
		//Hack this works for not but creates a lot of GC, check tonight.
			formatter.formatTo(Instant.ofEpochMilli(time), writer);//expensive call, must keep infrequent
			
	
		//} else {
			//just update seconds..
			
			
		//}
		
	}
	

	
}
