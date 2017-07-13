package com.ociweb.pronghorn.pipe;

import java.io.DataInput;
import java.io.InputStream;

import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;

public abstract class BlobReader extends InputStream implements DataInput {

	public abstract int available();
	
	public abstract <A extends Appendable> A readUTF(A target);
	
	public abstract String readUTFOfLength(int length);
	
	public abstract <A extends Appendable> A readUTFOfLength(int length, A target);
	
	public abstract long parse(TrieParserReader reader, TrieParser trie, int length);
	
	public abstract boolean equalUTF(byte[] equalText);
	
	public abstract boolean hasRemainingBytes();
	
	public abstract boolean equalBytes(byte[] bytes);
	
	public abstract boolean equalBytes(byte[] bytes, int bytesPos, int bytesLen);
	
	public abstract Object readObject();
	
	public abstract <A extends Appendable> A readPackedChars(A target);
	
	public abstract long readPackedLong();
	
	public abstract int readPackedInt();
	
	public abstract double readDecimalAsDouble();
	
	public abstract double readRationalAsDouble();	
	
	public abstract long readDecimalAsLong();
	
	public abstract short readPackedShort();
	
	public abstract byte readByte();
	
	public abstract short readShort();
	
	public abstract int skipBytes(int n);
	
	
}
