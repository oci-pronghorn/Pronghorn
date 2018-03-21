package com.ociweb.pronghorn.network.http;

import java.io.IOException;

import com.ociweb.pronghorn.network.config.HTTPHeader;
import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.util.hash.IntHashTable;
import com.ociweb.pronghorn.struct.BStructSchema;
import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.util.TrieParserReader;

public class HeaderUtil {

	public static void writeHeaderMiddle(DataOutputBlobWriter<NetPayloadSchema> writer, CharSequence implementationVersion) {
		boolean reportAgent = false;
		if (reportAgent) {
			DataOutputBlobWriter.write(writer, HeaderUtil.LINE_AND_USER_AGENT, 0, HeaderUtil.LINE_AND_USER_AGENT.length, Integer.MAX_VALUE);//DataOutputBlobWriter.encodeAsUTF8(writer,"\r\nUser-Agent: Pronghorn/");
			DataOutputBlobWriter.encodeAsUTF8(writer,implementationVersion);
		}
		
		DataOutputBlobWriter.write(writer, HeaderUtil.LINE_END, 0, HeaderUtil.LINE_END.length);
	}

	public static void writeHeaderEnding(DataOutputBlobWriter<NetPayloadSchema> writer, boolean keepOpen, long length) {

		if (keepOpen) {
			DataOutputBlobWriter.write(writer, HeaderUtil.CONNECTION_KEEP_ALIVE, 0, HeaderUtil.CONNECTION_KEEP_ALIVE.length);
			//DataOutputBlobWriter.encodeAsUTF8(writer,"\r\nConnection: keep-alive\r\n\r\n"); //double \r\b marks the end of the header
		} else {
			DataOutputBlobWriter.write(writer, HeaderUtil.CONNECTION_CLOSE, 0, HeaderUtil.CONNECTION_CLOSE.length, Integer.MAX_VALUE);
			//DataOutputBlobWriter.write(writer, LINE_END, 0, LINE_END.length, Integer.MAX_VALUE);
		}
		
		if (length>0) {
			DataOutputBlobWriter.write(writer, HeaderUtil.CONTENT_LENGTH, 0, HeaderUtil.CONTENT_LENGTH.length);
			Appendables.appendValue(writer, length);
			DataOutputBlobWriter.write(writer, HeaderUtil.LINE_END, 0, HeaderUtil.LINE_END.length);
		} else if (length<0) {
			DataOutputBlobWriter.write(writer, HeaderUtil.CONTENT_CHUNKED, 0, HeaderUtil.CONTENT_CHUNKED.length);
		}
		
		DataOutputBlobWriter.write(writer, HeaderUtil.LINE_END, 0, HeaderUtil.LINE_END.length);
	}

	public static void writeHeaderBeginning(byte[] hostBack, int hostPos, int hostLen, int hostMask,
			DataOutputBlobWriter<NetPayloadSchema> writer) {
		DataOutputBlobWriter.write(writer, HeaderUtil.REV11_AND_HOST, 0, HeaderUtil.REV11_AND_HOST.length); //encodeAsUTF8(writer," HTTP/1.1\r\nHost: ");
		DataOutputBlobWriter.write(writer, hostBack, hostPos, hostLen, hostMask);
	}

	final static byte[] LINE_END = "\r\n".getBytes();
	final static byte[] CONTENT_CHUNKED = "Transfer-Encoding: chunked".getBytes();
	final static byte[] CONTENT_LENGTH = "Content-Length: ".getBytes();
	final static byte[] CONNECTION_CLOSE = "Connection: close\r\n".getBytes();
	final static byte[] CONNECTION_KEEP_ALIVE = "Connection: keep-alive\r\n".getBytes();
	final static byte[] LINE_AND_USER_AGENT = "\r\nUser-Agent: Pronghorn/".getBytes();
	final static byte[] REV11_AND_HOST = " HTTP/1.1\r\nHost: ".getBytes();
	
	
	public static final IntHashTable headerTable(TrieParserReader localReader, HTTPSpecification<?,?,?,?> httpSpec, HTTPHeader ... headers) {
		
		IntHashTable headerToPosTable = IntHashTable.newTableExpectingCount(headers.length);		
		int count = 0;
		int i = headers.length;
		
		while (--i>=0) {
			int ord = headers[i].ordinal();
			assert(ord>=0) : "Bad header id";
			boolean ok = IntHashTable.setItem(headerToPosTable, HTTPHeader.HEADER_BIT | ord, HTTPHeader.HEADER_BIT | (count++));
			assert(ok);
		}
		
		return headerToPosTable;
	}
	
	public static void captureRequestedHeader(DataOutputBlobWriter<?> writer,
			final int indexOffsetCount, 
			final BStructSchema schmea, 
			final int structId, 
			final TrieParserReader trieReader, 
			final long fieldId) {
		
		//TODO: since the token process used the custom map for header parse
		//      needed by this record the value retured is already the needed one.
		
		//need to map this ordinal ID to a specific field in a struct??
		
		//schmea.getAssociatedObject(id);
		
		//is this header in the record??
		
		
		
//		//this value is specific to this Route and the headers requested.
//		int item = IntHashTable.getItem(headerToPositionTable,
//				      HTTPHeader.HEADER_BIT | headerId);
//	
//		if (0 == item) {
//		    //skip this data since the app module can not make use of it
//		    //this is the normal most frequent case                    
//		} else {
//			try {
//		    	//Id for the header
//		    	writer.writeShort(headerId);
//		    	//write values and write index to end of block??
//		    	int writePosition = writer.position();
//		    	                	
//				TrieParserReader.writeCapturedValuesToDataOutput(trieReader, writer, false);
//				if (writeIndex) {					
//					//we did not write index above so write here.
//					DataOutputBlobWriter.setIntBackData(writer, writePosition, 1 + (0xFFFF & item) + indexOffsetCount);
//				}					
//			} catch (IOException e) {
//				throw new RuntimeException(e);
//			}
//		}
	}

    @Deprecated
	public static void captureRequestedHeader(DataOutputBlobWriter<?> writer,
			final int indexOffsetCount, 
			final IntHashTable headerToPositionTable, 
			final boolean writeIndex,
			final TrieParserReader trieReader, 
			final int headerFieldOrdinalId) {
		//this value is specific to this Route and the headers requested.
		int item = IntHashTable.getItem(headerToPositionTable, HTTPHeader.HEADER_BIT | headerFieldOrdinalId);
	
		if (0 == item) {
		    //skip this data since the app module can not make use of it
		    //this is the normal most frequent case                    
		} else {
			
	    	//Id for the header
	    	writer.writeShort(headerFieldOrdinalId);
	    	//write values and write index to end of block??
	    	int writePosition = writer.position();
	    	                	
			TrieParserReader.writeCapturedValuesToDataOutput(trieReader, writer, false);
			if (writeIndex) {					
				//we did not write index above so write here.
				DataOutputBlobWriter.setIntBackData(writer, writePosition, 1 + (0xFFFF & item) + indexOffsetCount);
			}					
	
		}
	}

}
