package com.ociweb.pronghorn.ring.stream;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamingReadVisitorDebugDelegate implements StreamingReadVisitor {

	private StringBuilder tempStringBuilder =  new StringBuilder(128); 
	private ByteBuffer tempByteBuffer = ByteBuffer.allocate(512);
	private StreamingReadVisitor delegate;
	private Logger log = LoggerFactory.getLogger(StreamingReadVisitorDebugDelegate.class);
	
	public StreamingReadVisitorDebugDelegate(StreamingReadVisitor delegate) {
	    this.delegate = delegate;
	    
	}
	
	@Override
	public boolean paused() {
	    log.warn("paused");
		return delegate.paused();
	}

	@Override
	public void visitTemplateOpen(String name, long id) {
	    log.warn("visitTemplateOpen {}",name);
	    
	    delegate.visitTemplateOpen(name, id);
	}
	
	@Override
	public void visitTemplateClose(String name, long id) {
	    log.warn("visitTemplateClose {}",name);
	    delegate.visitTemplateClose(name, id);
	}

	@Override
	public void visitFragmentOpen(String name, long id, int cursor) {
	    log.warn("visitFragmentOpen {}",name);
	    delegate.visitFragmentOpen(name, id, cursor);
	}

	@Override
	public void visitFragmentClose(String name, long id) {
	    log.warn("visitFragmentClose {}",name);
	    delegate.visitFragmentClose(name, id);
	}

	@Override
	public void visitSequenceOpen(String name, long id, int length) {
	    log.warn("visitSequenceOpen {}",name);
	    delegate.visitSequenceOpen(name, id, length);
	}

	@Override
	public void visitSequenceClose(String name, long id) {
	    log.warn("visitSequenceClose {}",name);
	    delegate.visitSequenceClose(name,id);
	}

	@Override
	public void visitSignedInteger(String name, long id, int value) {
	    log.warn("visitSignedInteger {}",name);
	    delegate.visitSignedInteger(name,id,value);
	}

	@Override
	public void visitUnsignedInteger(String name, long id, long value) {
	    log.warn("visitUnsignedInteger {}",name);
	    delegate.visitUnsignedInteger(name,id,value);
	}

	@Override
	public void visitSignedLong(String name, long id, long value) {
	    log.warn("visitSignedLong {}",name);
	    delegate.visitSignedLong(name,id,value);
	}

	@Override
	public void visitUnsignedLong(String name, long id, long value) {	
	    log.warn("visitUnsignedLong {}",name);
	    delegate.visitUnsignedLong(name,id,value);
	}

	@Override
	public void visitDecimal(String name, long id, int exp, long mant) {
	    log.warn("visitDecimal {}",name);
	    delegate.visitDecimal(name,id,exp,mant);
	}

	@Override
	public void visitUTF8(String name, long id, Appendable value) {
	    log.warn("visitUTF8 {}",name);
	    delegate.visitUTF8(name,id,value);
	}

	@Override
	public Appendable targetASCII(String name, long id) {
	    log.warn("targetASCII {}",name);
	    
	    return delegate.targetASCII(name,id);
	}

	@Override
	public Appendable targetUTF8(String name, long id) {
	    log.warn("targetUTF8 {}",name);
	    
	    return delegate.targetUTF8(name,id);
	}

	@Override
	public ByteBuffer targetBytes(String name, long id, int length) {
	    log.warn("targetBytes {}",name);
	    
	    return delegate.targetBytes(name, id, length);
	}

	@Override
	public void visitBytes(String name, long id, ByteBuffer value) {
	    log.warn("visitBytes {}",name);
	    delegate.visitBytes(name, id, value);
	}

	@Override
	public void visitASCII(String name, long id, Appendable value) {
	    log.warn("visitASCII {}",name);
	    delegate.visitASCII(name, id, value);
	}

	@Override
	public void startup() {
	    log.warn("startup");
	    delegate.startup();
	}

	@Override
	public void shutdown() {
	    log.warn("shutdown");
	    delegate.shutdown();
	}

}
