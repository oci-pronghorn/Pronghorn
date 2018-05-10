package com.ociweb.pronghorn.pipe;

import com.ociweb.pronghorn.util.AppendableByteWriter;
import com.ociweb.pronghorn.util.ByteConsumer;

import java.io.Externalizable;
import java.io.ObjectOutput;
import java.io.OutputStream;

public abstract class ChannelWriter extends OutputStream implements ObjectOutput, Appendable, ByteConsumer, AppendableByteWriter {

	 abstract public StructuredWriter structured();

	/**
	 * Writes specified string to ChannelWriter
	 * @param s string to be written
	 */
	 abstract public void writeUTF(String s);

	/**
	 * Reads data written so far to ChannelWriter and determines how many bytes are remaining
	 * @return int of remaining bytes
	 */
	 abstract public int remaining();

	/**
	 * Gets length of data written so far to ChannelWriter and passes data as int
	 * @return int length of ChannelWriter
	 */
	 abstract public int length();

	 abstract public int absolutePosition();

	 abstract public void absolutePosition(int absolutePosition);

	 abstract public int position();

	 abstract public byte[] toByteArray();

	 abstract public Appendable append(CharSequence csq);
	 
	 abstract public Appendable append(CharSequence csq, int start, int end);
	 
	 abstract public Appendable append(char c);	 
	 
	 abstract public boolean reportObjectSizes(Appendable target);

	/**
	 * Writes data from object to ChannelWriter
	 * @param object object to get data from
	 */
	 abstract public void write(Externalizable object);

	/**
	 * Writes object to ChannelWriter
	 * @param object object to be written
	 */
	 abstract public void writeObject(Object object);

	/**
	 * Writes CharSequence to ChannelWriter
	 * @param s UTF8 text to be written
	 */
	 abstract public void writeUTF8Text(CharSequence s);

	/**
	 * Writes CharSequence to ChannelWriter
	 * @param s UTF text to be written
	 */
	 abstract public void writeUTF(CharSequence s);

	/**
	 * Writes CharSequence to ChannelWriter
	 * @param s ASCII to be written
	 */
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
