package org.slf4j.impl;

import org.slf4j.Logger;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBufferConfig;
import com.ociweb.pronghorn.ring.RingReader;
import com.ociweb.pronghorn.ring.RingWriter;


public class RingBufferLogger extends SimpleLogger implements Logger, RingBufferLoggerMessageConsumer {
	public static String PRIMARY_RING_SIZE = "primaryRingSizeInBits";
	public static String BYTE_RING_SIZE = "byteRingSizeInBits";
	
	private static final long serialVersionUID = 1816082662721088255L;
	
	private RingBuffer ring = null;
	private byte primaryRingSizeInBits = 7; //this ring is 2^7 eg 128
	private byte byteRingSizeInBits = 16;
	private Thread writeToFileThread = null;
	private boolean stopWriting = false;
	private RingBufferLoggerMessageConsumer consumer;
	private final int FRAG_LOC = FieldReferenceOffsetManager.LOC_CHUNKED_STREAM;
	private final int FRAG_FIELD = FieldReferenceOffsetManager.LOC_CHUNKED_STREAM_FIELD;

	RingBufferLogger(String name) {
		super(name);
		loadConfiguration();
		if(primaryRingSizeInBits!=0 && byteRingSizeInBits!=0) {
			RingBufferConfig config = new RingBufferConfig(primaryRingSizeInBits,byteRingSizeInBits,null,FieldReferenceOffsetManager.RAW_BYTES);
			ring = new RingBuffer(config);
			consumer = this;
		}
	}
	
	private void loadConfiguration() {
		primaryRingSizeInBits = (byte)getIntProperty(PRIMARY_RING_SIZE,primaryRingSizeInBits);
		byteRingSizeInBits=(byte)getIntProperty(BYTE_RING_SIZE,byteRingSizeInBits);
	}
	
	private String getSystemProperty(String name) {
	    String prop = null;
	    try {
	      prop = System.getProperty(name);
	    } catch (SecurityException e) {
	      ; // Ignore
	    }
	    return prop;
	}
	
	private int getIntProperty(String name, int defValue) {
		int value = defValue;
		String str = getSystemProperty(name);
		if(str!=null) {
			try {
				value = Integer.parseInt(str);
			} catch(Exception e) {
				value = defValue;
			}
		}
		return value;
	}
		
	
	@Override
	void write(StringBuilder buf, Throwable t) {
		if(ring==null) {
			writeToFile(buf,t);
		}
		else {
		    if (t != null) {
		    	buf.append(getTrace(t));
		    }
		    writeToRing(buf);
		}
	}
	private void writeToFile(StringBuilder buf, Throwable t) {
		super.write(buf, t);
	}
	
    @Override
    public void consumeMessage(StringBuffer message) {
		if(message!=null && message.length()>0) {
			super.write(new StringBuilder(message), null);
		}
	}

	private void writeToRing(StringBuilder buf) {
		synchronized(ring) {
	    while (true) {
        	if (RingWriter.tryWriteFragment(ring,FRAG_LOC)) {
         		RingWriter.writeASCII(ring, FRAG_FIELD, buf);
        		RingBuffer.publishWrites(ring);
	        	if(consumer!=null && writeToFileThread==null) {
	        		writeToFileThread = new Thread(new Runnable(){

	        			@Override
	        			public void run() {
	        		        while (true) {
	        		        	StringBuffer target = new StringBuffer();
	        			        if (RingReader.tryReadFragment(ring)) {
	        			        	RingReader.readASCII(ring, FRAG_FIELD, target);
        			        		consumer.consumeMessage(target);
	        			        }
	        			        else if(stopWriting) {
	        			        	return;
	        			        }
	        			        else {
	        			        	//unable to read so at this point
	        			        	//we can do other work and try again soon
	        			        	Thread.yield();		        			        }
	        		        }
	        			}
	        		});
	        		writeToFileThread.start();
	        	}
	        	break;
        	}
        	else {
        		// Let consumer to free buffer from the previous logs
        		Thread.yield();
        	}
	    }
		}
	}
	
	private StringBuilder getTrace(Throwable t) {
		StringBuilder buff = new StringBuilder();
		buff.append("\t").append(t.getMessage()).append("\n");
		for(StackTraceElement trace: t.getStackTrace()) {
			buff.append("\t").append(trace.toString()).append("\n");
		}
		if(t.getCause()!=null) {
			buff.append("Coused by: ").append(getTrace(t.getCause()));
		}
		return buff;
	}
	public RingBuffer getRingBuffer() {
		return ring;
	}

	public void setRingBuffer(RingBuffer ring) {
		this.ring = ring;
	}
	
	public void stopWriting() {
		stopWriting = true;
		if(writeToFileThread!=null) {
			while(writeToFileThread.isAlive()) {
				Thread.yield();
			}
		}
	}

	public void setConsumer(RingBufferLoggerMessageConsumer consumer) {
		this.consumer = consumer;
	}


}
