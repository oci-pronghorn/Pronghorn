package com.ociweb.pronghorn.pipe.stream;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamingReadVisitorDebugDelegate implements StreamingReadVisitor {

	private StringBuilder tempStringBuilder =  new StringBuilder(128); 
	private ByteBuffer tempByteBuffer = ByteBuffer.allocate(512);
	private StreamingReadVisitor delegate;
	private Logger log = LoggerFactory.getLogger(StreamingReadVisitorDebugDelegate.class);
	private long templateCount;
	private int tab;
	private String tabs = "                                                    ";
	
	public StreamingReadVisitorDebugDelegate(StreamingReadVisitor delegate) {
	    this.delegate = delegate;
	    
	}
	
	private String buildTab() {
	    return tabs.substring(0, tab);
	}
	
	@Override
	public boolean paused() {
	    log.warn(buildTab()+"paused");
		return delegate.paused();
	}

	@Override
	public void visitTemplateOpen(String name, long id) {	    
	    log.warn(buildTab()+"visitTemplateOpen {}   ordinal:{}",name,++templateCount);	    
	    delegate.visitTemplateOpen(name, id);
	    tab++;
	}
	
	@Override
	public void visitTemplateClose(String name, long id) {
	    tab--;
	    log.warn(buildTab()+"visitTemplateClose {}",name);
	    delegate.visitTemplateClose(name, id);
	}

	@Override
	public void visitFragmentOpen(String name, long id, int cursor) {
	    log.warn(buildTab()+"visitFragmentOpen {}",name);
	    delegate.visitFragmentOpen(name, id, cursor);
	    tab++;
	}

	@Override
	public void visitFragmentClose(String name, long id) {
	    
	    tab--;
	    log.warn(buildTab()+"visitFragmentClose {}",name);
	    delegate.visitFragmentClose(name, id);
	}

	@Override
	public void visitSequenceOpen(String name, long id, int length) {
	    tab++;
	    log.warn(buildTab()+"visitSequenceOpen {} length {}",name,length);
	    delegate.visitSequenceOpen(name, id, length);
	}

	@Override
	public void visitSequenceClose(String name, long id) {
	    tab--;
	    log.warn(buildTab()+"visitSequenceClose {}",name);
	    delegate.visitSequenceClose(name,id);
	}

	@Override
	public void visitSignedInteger(String name, long id, int value) {
	    log.warn(buildTab()+"visitSignedInteger {}",name);
	    delegate.visitSignedInteger(name,id,value);
	}

	@Override
	public void visitUnsignedInteger(String name, long id, long value) {
	    log.warn(buildTab()+"visitUnsignedInteger {} value {}", name, value);
	    delegate.visitUnsignedInteger(name,id,value);
	}

	@Override
	public void visitSignedLong(String name, long id, long value) {
	    log.warn(buildTab()+"visitSignedLong {}",name);
	    delegate.visitSignedLong(name,id,value);
	}

	@Override
	public void visitUnsignedLong(String name, long id, long value) {	
	    log.warn(buildTab()+"visitUnsignedLong {} value {}",name,value);
	    delegate.visitUnsignedLong(name,id,value);
	}

	@Override
	public void visitDecimal(String name, long id, int exp, long mant) {
	    log.warn(buildTab()+"visitDecimal {}",name);
	    delegate.visitDecimal(name,id,exp,mant);
	}

	@Override
	public void visitUTF8(String name, long id, CharSequence value) {
	    log.warn(buildTab()+"visitUTF8 {}",name);
	    delegate.visitUTF8(name,id,value);
	}

	@Override
	public Appendable targetASCII(String name, long id) {
	    log.warn(buildTab()+"targetASCII {}",name);
	    
	    return delegate.targetASCII(name,id);
	}

	@Override
	public Appendable targetUTF8(String name, long id) {
	    log.warn(buildTab()+"targetUTF8 {}",name);
	    
	    return delegate.targetUTF8(name,id);
	}

	@Override
	public ByteBuffer targetBytes(String name, long id, int length) {
	    log.warn(buildTab()+"targetBytes {}",name);
	    
	    return delegate.targetBytes(name, id, length);
	}

	@Override
	public void visitBytes(String name, long id, ByteBuffer value) {
	    log.warn(buildTab()+"visitBytes {}",name);
	    delegate.visitBytes(name, id, value);
	}

	@Override
	public void visitASCII(String name, long id, CharSequence value) {
	    log.warn(buildTab()+"visitASCII {}",name);
	    delegate.visitASCII(name, id, value);
	}

	@Override
	public void startup() {
	    log.warn(buildTab()+"startup");
	    delegate.startup();
	}

	@Override
	public void shutdown() {
	    log.warn(buildTab()+"shutdown");
	    delegate.shutdown();
	}

}
