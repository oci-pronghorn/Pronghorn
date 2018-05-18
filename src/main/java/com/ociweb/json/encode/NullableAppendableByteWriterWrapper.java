package com.ociweb.json.encode;

import com.ociweb.pronghorn.util.AppendableByteWriter;

public class NullableAppendableByteWriterWrapper implements AppendableByteWriter<NullableAppendableByteWriterWrapper> {
	
	public boolean wasNull;
	public AppendableByteWriter<?> externalWriter;
	protected boolean needsQuote;
		
	@Override
	public void reset() {
		externalWriter.reset();
		wasNull = false;
		needsQuote = true;
				
	}

	@Override
	public NullableAppendableByteWriterWrapper append(CharSequence csq) {
	   	if (null !=csq) {
			assert(!wasNull) : "Can not write text after writing a null";
			if (needsQuote) {
				externalWriter.writeByte('"');
				needsQuote = false;
			}
			externalWriter.append(csq);			
		} else {
			wasNull = true;
			needsQuote = false;
			externalWriter.reset();
		}
		return this;
	}

	@Override
	public NullableAppendableByteWriterWrapper append(CharSequence csq, int start, int end) {
		assert(!wasNull) : "Can not write text after writing a null";
		if (needsQuote) {
			externalWriter.writeByte('"');
			needsQuote = false;
		}
		externalWriter.append(csq,start,end);
		return this;
	}

	@Override
	public NullableAppendableByteWriterWrapper append(char c) {
		assert(!wasNull) : "Can not write text after writing a null";
		if (needsQuote) {
			externalWriter.writeByte('"');
			needsQuote = false;
		}
		externalWriter.append(c);
		return this;
	}

	@Override
	public void write(byte[] b) {
		assert(!wasNull) : "Can not write text after writing a null";
		if (needsQuote) {
			externalWriter.writeByte('"');
			needsQuote = false;
		}
		externalWriter.write(b);
	}

	@Override
	public void write(byte[] b, int pos, int len) {
		assert(!wasNull) : "Can not write text after writing a null";
		if (needsQuote) {
			externalWriter.writeByte('"');
			needsQuote = false;
		}
		externalWriter.write(b,pos,len);
	}

	@Override
	public void writeByte(int asciiChar) {
		assert(!wasNull) : "Can not write text after writing a null";
		if (needsQuote) {
			externalWriter.writeByte('"');
			needsQuote = false;
		}
		externalWriter.writeByte(asciiChar);
	}
	
}
