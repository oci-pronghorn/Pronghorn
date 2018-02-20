package com.ociweb.pronghorn.pipe;

import java.io.Externalizable;
import java.io.InputStream;
import java.io.ObjectInput;

import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;

public abstract class ChannelReader extends InputStream implements ObjectInput {

	public abstract int available();
	
	public abstract <A extends Appendable> A readUTF(A target);
	
	public abstract String readUTFFully();
	
	public abstract String readUTFOfLength(int length);
	
	public abstract <A extends Appendable> A readUTFOfLength(int length, A target);
	
	public abstract long parse(TrieParserReader reader, TrieParser trie, int length);
	
	public abstract boolean equalUTF(byte[] equalText);
	
	public abstract String readUTF();
	
	public abstract boolean hasRemainingBytes();
	
	public abstract int read(byte b[]);
	
	public abstract int read(byte b[], int off, int len);
	
	public abstract boolean equalBytes(byte[] bytes);
	
	public abstract boolean equalBytes(byte[] bytes, int bytesPos, int bytesLen);
	
	public abstract void readInto(Externalizable target);
	
	public abstract void readInto(ChannelWriter writer, int length);
	
	public abstract Object readObject();
	
	public abstract <A extends Appendable> A readPackedChars(A target);
	
	public abstract boolean wasPackedNull();
	
	public abstract boolean wasDecimalNull();
	
	public abstract long readPackedLong();
	
	public abstract int readPackedInt();
	
	public abstract double readDecimalAsDouble();
	
	public abstract double readRationalAsDouble();	
	
	public abstract long readDecimalAsLong();
	
	public abstract short readPackedShort();
	
	public abstract byte readByte();
	
	public abstract short readShort();
	
	public abstract int readInt();
	
	public abstract long readLong();
	
	public abstract double readDouble();
	
	public abstract float readFloat();
	
	public abstract int read();
	
	public abstract boolean readBoolean();
	
	public abstract boolean wasBooleanNull();
	
	public abstract int skipBytes(int n);
	
	public abstract int absolutePosition();

	public abstract void absolutePosition(int i);
}
