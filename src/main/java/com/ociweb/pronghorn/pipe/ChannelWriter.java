package com.ociweb.pronghorn.pipe;

import com.ociweb.pronghorn.util.AppendableByteWriter;
import com.ociweb.pronghorn.util.ByteConsumer;

import java.io.Externalizable;
import java.io.ObjectOutput;
import java.io.OutputStream;

public abstract class ChannelWriter extends OutputStream implements ObjectOutput, Appendable, ByteConsumer, AppendableByteWriter<ChannelWriter> {

	 abstract public StructuredWriter structured();

	/**
	 * Writes specified string to ChannelWriter
	 * @param s UTF to be written
	 */
	 abstract public void writeUTF(String s);

	/**
	 * Reads data written so far to ChannelWriter and determines how many bytes are remaining
	 * @return int of remaining bytes
	 */
	 abstract public int remaining();

	/**
	 * Gets length of data written so far to ChannelWriter and passes data as int
	 * @return length of ChannelWriter
	 */
	 abstract public int length();

	 abstract public int absolutePosition();

	 abstract public void absolutePosition(int absolutePosition);

	/**
	 * Gets position of data written so far to ChannelWriter and passes data as int
	 * @return position of data
	 */
	 abstract public int position();

	/**
	 * Gets byte data written so far to ChannelWriter and passes data to byte array
	 * @return byte array
	 */
	 abstract public byte[] toByteArray();
	 
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

	/**
	 * Writes longs to ChannelWriter
	 * @param numerator long to be written
	 * @param denominator long to be written
	 */
	 abstract public void writeRational(long numerator, long denominator);

	/**
	 * Writes long and byte to ChannelWriter
	 * @param m long to be written
	 * @param e byte to be written
	 * //TODO: more descriptive args?
	 */
	 abstract public void writeDecimal(long m, byte e);

	/**
	 * Writes array of UTFs to ChannelWriter
	 * @param utfs string to be written
	 */
	 abstract public void writeUTFArray(String[] utfs);

	 abstract public void write(byte[] source, int sourceOff, int sourceLen, int sourceMask);

	/**
	 * Writes packed CharSequence to ChannelWriter
	 * @param text text to be written
	 */
	 abstract public void writePackedString(CharSequence text);

	/**
	 * Writes packed long to ChannelWriter
	 * @param value long to be written
	 */
	 abstract public void writePackedLong(long value);

	/**
	 * Writes packed int to ChannelWriter
	 * @param value int to be written
	 */
	 abstract public void writePackedInt(int value);

	/**
	 * Writes packed short to ChannelWriter
	 * @param value short to be written
	 */
	 abstract public void writePackedShort(short value);

	/**
	 * Writes double to ChannelWriter
	 * @param value double to be written
	 */
	 abstract public void writeDouble(double value);

	/**
	 * Writes float to ChannelWriter
	 * @param value float to be written
	 */
	abstract public void writeFloat(float value);

	/**
	 * Writes long to ChannelWriter
	 * @param v long to be written
	 */
	abstract public void writeLong(long v);

	/**
	 * Writes int to ChannelWriter
	 * @param v int to be written
	 */
	 abstract public void writeInt(int v);

	/**
	 * The low 16 bits are written to ChannelWriter
	 * @param v int to be converted to short and written
	 */
	 abstract public void writeShort(int v);

	/**
	 * The low 8 bits are written to ChannelWriter
	 * @param v int to be converted to byte and written
	 */
	 abstract public void writeByte(int v);

	/**
	 * Writes boolean to ChannelWriter
	 * @param v boolean to be written
	 */
	 abstract public void writeBoolean(boolean v);

	 abstract public void writeBooleanNull();

	 abstract public void writePackedNull();
	 
	 abstract public void write(byte b[], int off, int len);

	/**
	 * Writes byte array to ChannelWriter
	 * @param b byte array to be written
	 */
	 abstract public void write(byte b[]);
	 
	 abstract public void writeUTF8Text(CharSequence s, int pos, int len);
	 
	 abstract public int closeLowLevelField();
	 abstract public int closeHighLevelField(int targetFieldLoc);
		
}
