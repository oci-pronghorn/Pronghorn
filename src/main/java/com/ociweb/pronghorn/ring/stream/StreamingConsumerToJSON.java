package com.ociweb.pronghorn.ring.stream;

import java.io.PrintStream;
import java.nio.ByteBuffer;

public class StreamingConsumerToJSON implements StreamingConsumer {

	StringBuilder temp =  new StringBuilder(128); 
	PrintStream out;
	int depth = 0;
	int step = 2;
	
	public StreamingConsumerToJSON(PrintStream out) {
		this.out = out;
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
	public void visitFragmentOpen(String name, long id) {
		writeTab();
		out.println("{\""+name+"\":");		
		depth += step;
	}

	@Override
	public void visitFragmentClose(String name, long id) {
		depth -= step;
		writeTab();
		out.println("{");		
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
	public void visitOptionalSignedInteger(String name, long id, int value) {
		writeTab();
		out.println("{\""+name+"\":"+Integer.valueOf(value)+"}");
	}

	@Override
	public void visitOptionalUnsignedInteger(String name, long id, long value) {
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
	public void visitOptinoalSignedLong(String name, long id, long value) {
		writeTab();
		out.println("{\""+name+"\":"+Long.valueOf(value)+"}");
	}

	@Override
	public void visitOptionalUnsignedLong(String name, long id, long value) {
		writeTab();
		out.println("{\""+name+"\":"+Long.valueOf(value)+"}"); //TODO: this is not strictly right and can be negative!!
	}

	@Override
	public void visitDecimal(String name, long id, int exp, long mant) {
		writeTab();
		out.println("{\""+name+"\":["+Integer.valueOf(exp)+","+Long.valueOf(mant)+"]}");
	}

	@Override
	public void visitOptionalDecimal(String name, long id, int exp, long mant) {
		writeTab();
		out.println("{\""+name+"\":["+Integer.valueOf(exp)+","+Long.valueOf(mant)+"]}");
	}

	@Override
	public Appendable targetASCII(String name, long id) {
		temp.setLength(0);
		return temp;
	}

	@Override
	public void visitASCII(String name, long id, Appendable value) {
		writeTab();
		out.println("{\""+name+"\":\""+value+"\"}");
	}

	@Override
	public Appendable targetOptionalASCII(String name, long id) {
		temp.setLength(0);
		return temp;
	}

	@Override
	public void visitOptionalASCII(String name, long id, Appendable value) {
		writeTab();
		out.println("{\""+name+"\":\""+value+"\"}");
	}

	@Override
	public Appendable targetUTF8(String name, long id) {
		temp.setLength(0);
		return temp;
	}

	@Override
	public void visitUTF8(String name, long id, Appendable value) {
		writeTab();
		out.println("{\""+name+"\":\""+value+"\"}");
	}

	@Override
	public Appendable targetOptionalUTF8(String name, long id) {
		temp.setLength(0);
		return temp;
	}

	@Override
	public void visitOptionalUTF8(String name, long id, Appendable value) {
		writeTab();
		out.println("{\""+name+"\":\""+value+"\"}");
		
	}

	@Override
	public ByteBuffer targetBytes(String name, long id) {
		throw new UnsupportedOperationException("Still not sure how big to make this buffer, may be the wrong data structure"); //TODO: AA, follow up on this.
	}

	@Override
	public void visitBytes(String name, long id, ByteBuffer target) {
	}

	@Override
	public ByteBuffer targetOptionalBytes(String name, long id) {
		throw new UnsupportedOperationException("Still not sure how big to make this buffer, may be the wrong data structure"); //TODO: AA, follow up on this.
	}

	@Override
	public void visitOptionalBytes(String name, long id, ByteBuffer target) {
	}

}
