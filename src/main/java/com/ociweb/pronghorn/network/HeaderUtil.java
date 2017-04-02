package com.ociweb.pronghorn.network;

import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.util.Appendables;

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

}
