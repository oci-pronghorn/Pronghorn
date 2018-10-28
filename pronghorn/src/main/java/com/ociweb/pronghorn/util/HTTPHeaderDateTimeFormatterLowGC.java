package com.ociweb.pronghorn.util;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import com.ociweb.pronghorn.pipe.ChannelWriter;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;

public class HTTPHeaderDateTimeFormatterLowGC {

//	String HTTP_RESPONSE_DATE_HEADER = "EEE, dd MMM yyyy HH:mm:ss zzz";		  
//    Calendar calendar = Calendar.getInstance();
//    SimpleDateFormat dateFormat = new SimpleDateFormat(HTTP_RESPONSE_DATE_HEADER, Locale.US);
//    dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
//    String dateString = dateFormat.format(calendar.getTime());
    
	//////// constants
	private final String HTTP_HEADER_DATE_FORMAT;
	private final int SECONDS_OFFSET; //where the seconds begin
	private final int SECONDS_LENGTH; //where the seconds begin
	////////////////////////////////////
	
	private final DateTimeFormatter formatter;
	private long validRange = 0;

	Pipe<RawDataSchema> temp;
	
	public HTTPHeaderDateTimeFormatterLowGC() {

			HTTP_HEADER_DATE_FORMAT = "EEE, dd MMM yyyy HH:mm:ss";
			SECONDS_OFFSET  = 23; //where the seconds begin
			SECONDS_LENGTH  = 2;  //where the seconds ends
	
					
		formatter = DateTimeFormatter
				    .ofPattern(HTTP_HEADER_DATE_FORMAT)
				    .withZone( ZoneOffset.UTC );
	
		temp = RawDataSchema.instance.newPipe(2, 32);
		temp.initBuffers();
		
	}
		
	public void write(long time, ChannelWriter writer) {
		
		long localMinute = time/60_000L;
		
		if (localMinute != validRange) {
						
		    //this is so we know that we are in the same minute next time
		    validRange = localMinute; 
		    		    
		    temp.reset();
		    int size = Pipe.addMsgIdx(temp, RawDataSchema.MSG_CHUNKEDSTREAM_1);
		    
		    DataOutputBlobWriter<RawDataSchema> targetStream = Pipe.openOutputStream(temp);
		    		    
		    //expensive call, must keep infrequent
			formatter.formatTo(Instant.ofEpochMilli(time), targetStream);
			targetStream.append(" GMT");
			targetStream.replicate(writer);
			
			DataOutputBlobWriter.closeLowLevelField(targetStream);
			Pipe.confirmLowLevelWrite(temp);
			Pipe.publishWrites(temp);
				
		} else {
			//just update seconds but use the rest
			
			Pipe.markTail(temp);
			Pipe.takeMsgIdx(temp);
			DataInputBlobReader<RawDataSchema> inStream = Pipe.openInputStream(temp);
			inStream.readInto(writer, SECONDS_OFFSET);
			inStream.skip(SECONDS_LENGTH);
			
			long sec = (time%60_000L)/1_000L;					
			Appendables.appendFixedDecimalDigits(writer, sec, 10);
				
			inStream.readInto(writer, inStream.available());
			
			Pipe.resetTail(temp);
			
		}
		
	}
	
	public void write(long time, Appendable target) {
		
		if (target instanceof ChannelWriter) {
			write(time, (ChannelWriter)target);			
		} else {		
			slowWrite(time, target);
		}
	}

	private void slowWrite(long time, Appendable target) {
		long localMinute = time/60_000L;
		
		if (localMinute != validRange) {
						
		    //this is so we know that we are in the same minute next time
		    validRange = localMinute; 
		    		    
		    temp.reset();
		    int size = Pipe.addMsgIdx(temp, RawDataSchema.MSG_CHUNKEDSTREAM_1);
		    
		    DataOutputBlobWriter<RawDataSchema> targetStream = Pipe.openOutputStream(temp);
		    		    
		    //expensive call, must keep infrequent
			formatter.formatTo(Instant.ofEpochMilli(time), targetStream);
			targetStream.append(" GMT");
			targetStream.replicate(target);
						
			DataOutputBlobWriter.closeLowLevelField(targetStream);
			Pipe.confirmLowLevelWrite(temp);
			Pipe.publishWrites(temp);
				
		} else {
			//just update seconds but use the rest
			
			Pipe.markTail(temp);
			Pipe.takeMsgIdx(temp);
			DataInputBlobReader<RawDataSchema> inStream = Pipe.openInputStream(temp);
			inStream.readUTFOfLength(SECONDS_OFFSET, target);
			inStream.skip(SECONDS_LENGTH);
			
			long sec = (time%60_000L)/1_000L;					
			Appendables.appendFixedDecimalDigits(target, sec, 10);
						
			inStream.readUTFOfLength(inStream.available(), target);
				
			Pipe.resetTail(temp);
			
		}
	}
	
}
