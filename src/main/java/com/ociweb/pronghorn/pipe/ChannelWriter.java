package com.ociweb.pronghorn.pipe;

import java.io.Externalizable;
import java.io.ObjectOutput;
import java.io.OutputStream;

import com.ociweb.pronghorn.util.AppendableByteWriter;
import com.ociweb.pronghorn.util.ByteConsumer;

public abstract class ChannelWriter extends OutputStream implements ObjectOutput, Appendable, ByteConsumer, AppendableByteWriter {

	 abstract public StructuredWriter structured();
	
	 abstract public void writeUTF(String s);
	 
	 abstract public int remaining();
	 
	 abstract public int length();

	 abstract public int absolutePosition();

	 abstract public void absolutePosition(int absolutePosition);

	 abstract public int position();

	 abstract public byte[] toByteArray();
	 
	 abstract public Appendable append(CharSequence csq);
	 
	 abstract public Appendable append(CharSequence csq, int start, int end);
	 
	 abstract public Appendable append(char c);	 
	 
	 abstract public boolean reportObjectSizes(Appendable target);
	 
	 abstract public void write(Externalizable object);
	 
	 abstract public void writeObject(Object object);
	 
	 abstract public void writeUTF8Text(CharSequence s);
	 
	 abstract public void writeUTF(CharSequence s);
	 
	 abstract public void writeASCII(CharSequence s);
	 	 
	 abstract public void writeRational(long numerator, long denominator);
	 
	 abstract public void writeDecimal(long m, byte e);
	 
	 abstract public void writeUTFArray(String[] utfs);
	 
	 abstract public void write(byte[] source, int sourceOff, int sourceLen, int sourceMask);
	 
	 abstract public void writePackedString(CharSequence text);
	 
	 abstract public void writePackedLong(long value);
	 
	 abstract public void writePackedInt(int value);
	 
	 abstract public void writePackedShort(short value);
	 
	 abstract public void writeDouble(double value);
	 
	 abstract public void writeFloat(float value);	 
	 
	 abstract public void writeLong(long v);
	 
	 abstract public void writeInt(int v);
	 
	 abstract public void writeShort(int v);

	 abstract public void writeByte(int v);
	 
	 abstract public void writeBoolean(boolean v);
	 
	 abstract public void writeBooleanNull();

	 abstract public void writePackedNull();
	 
	 abstract public void write(byte b[], int off, int len);
	 
	 abstract public void write(byte b[]);
	 
	 abstract public void writeUTF8Text(CharSequence s, int pos, int len);
	 
		
}
