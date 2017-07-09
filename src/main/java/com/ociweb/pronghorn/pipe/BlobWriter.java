package com.ociweb.pronghorn.pipe;

import java.io.DataOutput;
import java.io.OutputStream;

import com.ociweb.pronghorn.util.ByteConsumer;

public abstract class BlobWriter extends OutputStream implements DataOutput, Appendable, ByteConsumer {

	 abstract public void writeUTF(String s);
	 
	 abstract public int remaining();
	 
	 abstract public int length();
	 
	 abstract public byte[] toByteArray();
	 
	 abstract public void writeObject(Object object);
	 
	 abstract public void writeUTF8Text(CharSequence s);
	 
	 abstract public void writeUTF(CharSequence s);
	 
	 abstract public void writeASCII(CharSequence s);
	 
	 abstract public void writeByteArray(byte[] bytes);
	 
	 abstract public void writeCharArray(char[] chars);
	 
	 abstract public void writeIntArray(int[] ints);
	 
	 abstract public void writeLongArray(long[] longs);
	 
	 abstract public void writeDoubleArray(double[] doubles);
	 
	 abstract public void writeFloatArray(float[] floats);
	 
	 abstract public void writeShortArray(short[] shorts);
	 
	 abstract public void writeBooleanArray(boolean[] booleans);
	 
	 abstract public void writeUTFArray(String[] utfs);
	 
	 abstract public void write(byte[] source, int sourceOff, int sourceLen, int sourceMask);
	 
	 abstract public void writePackedString(CharSequence text);
	 
	 abstract public void writePackedLong(long value);
	 
	 abstract public void writePackedInt(int value);
	 
	 abstract public void writePackedShort(short value);
	 
	 
}
