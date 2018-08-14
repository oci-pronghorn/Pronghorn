package com.ociweb.pronghorn.pipe.util;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import com.ociweb.pronghorn.pipe.ChannelWriter;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.util.Appendables;

public class ISOTimeFormatterLowGC {
	
	//////// constants
	private final String YYYY_MM_DD_HH_MM_SS_SSS_Z;
	private final int SECONDS_OFFSET; //where the seconds begin
	private final int SECONDS_LENGTH; //where the seconds begin
	private final byte SECONDS_PLACES; //sub second places of accuracy (always negative)
	////////////////////////////////////
	
	private final DateTimeFormatter formatter;
	private long validRange = 0;

	Pipe<RawDataSchema> temp;
	
	public ISOTimeFormatterLowGC() {
		this(false);
	}
	
	public ISOTimeFormatterLowGC(boolean fileNameSafe) {
		
		if (fileNameSafe) {
			YYYY_MM_DD_HH_MM_SS_SSS_Z = "yyyyMMddHHmmssSSS";
			SECONDS_OFFSET  = 12; //where the seconds begin
			SECONDS_LENGTH  = 5; //where the seconds begin
			SECONDS_PLACES =  0; //sub second places of accuracy (always negative)
		} else {
			YYYY_MM_DD_HH_MM_SS_SSS_Z = "yyyy-MM-dd HH:mm:ss.SSS z";
			SECONDS_OFFSET  = 17; //where the seconds begin
			SECONDS_LENGTH  = 6; //where the seconds begin
			SECONDS_PLACES = -3; //sub second places of accuracy (always negative)
		}
		
		
		formatter = DateTimeFormatter
				    .ofPattern(YYYY_MM_DD_HH_MM_SS_SSS_Z)
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
			formatter.formatTo(Instant.ofEpochMilli(time), targetStream);// 2018-04-24 17:43:04.490 Z
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
			
			long sec = time%60_000L;
			if (SECONDS_PLACES != 0) {
				if (sec<10_000) {
					writer.writeByte('0');
				}
				assert(sec<100_000);
				Appendables.appendDecimalValue(writer, sec, SECONDS_PLACES);
			} else {			
				Appendables.appendFixedDecimalDigits(writer, sec, 10000);
			}
			
			inStream.readInto(writer, inStream.available());
			
			Pipe.resetTail(temp);
			
		}
		
	}
	
	public void write(long time, Appendable target) {
		
		long localMinute = time/60_000L;
		
		if (localMinute != validRange) {
						
		    //this is so we know that we are in the same minute next time
		    validRange = localMinute; 
		    		    
		    temp.reset();
		    int size = Pipe.addMsgIdx(temp, RawDataSchema.MSG_CHUNKEDSTREAM_1);
		    
		    DataOutputBlobWriter<RawDataSchema> targetStream = Pipe.openOutputStream(temp);
		    		    
		    //expensive call, must keep infrequent
			formatter.formatTo(Instant.ofEpochMilli(time), targetStream);// 2018-04-24 17:43:04.490 Z
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
			
			long sec = time%60_000L;
			if (SECONDS_PLACES != 0) {
				if (sec<10_000) {
					try {
						target.append('0');
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}
				assert(sec<100_000);
				Appendables.appendDecimalValue(target, sec, SECONDS_PLACES);
			} else {			
				Appendables.appendFixedDecimalDigits(target, sec, 10000);
			}
			inStream.readUTFOfLength(inStream.available(), target);
				
			Pipe.resetTail(temp);
			
		}
		
	}
	
}
