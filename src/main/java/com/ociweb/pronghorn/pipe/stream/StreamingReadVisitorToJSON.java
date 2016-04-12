package com.ociweb.pronghorn.pipe.stream;

import java.io.PrintStream;
import java.nio.ByteBuffer;

public class StreamingReadVisitorToJSON implements StreamingReadVisitor {

	final StringBuilder tempStringBuilder;
	ByteBuffer tempByteBuffer;
	
	PrintStream out;
	int depth = 0;
	int step = 2;
	
	public StreamingReadVisitorToJSON(PrintStream out) { //TODO: this should have been Appendable not PrintStream.
		this(out,4096, 256);
	}
	
	public StreamingReadVisitorToJSON(PrintStream out, int maxBytesSize, int maxStringSize) {
	    this.out = out;
	    this.tempByteBuffer = ByteBuffer.allocate(maxBytesSize);
	    this.tempStringBuilder =  new StringBuilder(maxStringSize);
	    
	}
	
	
	@Override
	public boolean paused() {
		return false; //not used in this case because we block on out
	}

	private void writeTab() {
		int j = depth;
		while (--j>=0) {
			out.print(' ');
		}
	}
	
	@Override
	public void visitTemplateOpen(String name, long id) {
		//no tab needed here
		out.println("{\""+name+"\":");			
		depth = step;
	}
	
	@Override
	public void visitTemplateClose(String name, long id) {
		depth -= step;
		writeTab();
		out.println("}");
	}

	@Override
	public void visitFragmentOpen(String name, long id, int cursor) {
		writeTab();
		out.println("{\""+name+"\":");		
		depth += step;
	}

	@Override
	public void visitFragmentClose(String name, long id) {
		depth -= step;
		writeTab();
		out.println("}");		
	}

	@Override
	public void visitSequenceOpen(String name, long id, int length) {
		writeTab();
		out.println("[");		
		depth += step;
	}

	@Override
	public void visitSequenceClose(String name, long id) {
		depth -= step;
		writeTab();
		out.println("]");
	}

	@Override
	public void visitSignedInteger(String name, long id, int value) {
		writeTab();
		out.println("{\""+name+"\":"+Integer.valueOf(value)+"}");
	}

	@Override
	public void visitUnsignedInteger(String name, long id, long value) {
		writeTab();
		out.println("{\""+name+"\":"+Long.valueOf(value)+"}");
	}

	@Override
	public void visitSignedLong(String name, long id, long value) {
		writeTab();
		out.println("{\""+name+"\":"+Long.valueOf(value)+"}");
	}

	@Override
	public void visitUnsignedLong(String name, long id, long value) {
		writeTab();
		out.println("{\""+name+"\":"+Long.valueOf(value)+"}"); //TODO: this is not strictly right and can be negative!!
	}

	@Override
	public void visitDecimal(String name, long id, int exp, long mant) {
		writeTab();
		out.println("{\""+name+"\":["+Integer.valueOf(exp)+","+Long.valueOf(mant)+"]}");
	}

	@Override
	public Appendable targetASCII(String name, long id) {
		tempStringBuilder.setLength(0);
		return tempStringBuilder;
	}

	@Override
	public void visitASCII(String name, long id, Appendable value) {
		writeTab();
		out.println("{\""+name+"\":\""+value+"\"}");
	}

	@Override
	public Appendable targetUTF8(String name, long id) {
		tempStringBuilder.setLength(0);
		return tempStringBuilder;
	}

	@Override
	public void visitUTF8(String name, long id, Appendable value) {
		writeTab();
		out.println("{\""+name+"\":\""+value+"\"}");
	}

	@Override
	public ByteBuffer targetBytes(String name, long id, int length) {
		tempByteBuffer.clear();
		if (tempByteBuffer.capacity()<length) {
			tempByteBuffer = ByteBuffer.allocate(length*2);
		}
		return tempByteBuffer;
	}

	@Override
	public void visitBytes(String name, long id, ByteBuffer value) {
	    writeTab();
        out.print("{\""+name+"\":\"");
        value.flip();
        
        while (value.hasRemaining()) {//TODO: needs clean up !!
            String tmp = "00"+Integer.toHexString(value.get());
            out.print("0x");
            out.print(tmp.substring(tmp.length()-2)  );
            if (value.hasRemaining()) {
                out.print(",");
            }

        }
        out.println("\"}");
        
        
		//undefined how we should send a binary block to JSON
	}

	@Override
	public void startup() {
	}

	@Override
	public void shutdown() {
	}

}
