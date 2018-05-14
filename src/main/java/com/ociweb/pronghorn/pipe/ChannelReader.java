package com.ociweb.pronghorn.pipe;

import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;

import java.io.Externalizable;
import java.io.InputStream;
import java.io.ObjectInput;

public abstract class ChannelReader extends InputStream implements ObjectInput, TextReader {

	/**
	 * Will give how many bytes are available right now
	 * @return the number of bytes bytes
	 */
	public abstract int available();

	/**
	 * Reads bytes from ChannelReader depending on leading short for length
	 * assuming bytes are UTF-8 encoded passes them to target
	 * @param target receiver of converted bytes
	 * @return target with UTF bytes
	 */
	public abstract <A extends Appendable> A readUTF(A target);

	/**
	 * Reads all UTF bytes from ChannelReader, does not use leading short.
	 * @return String of UTF bytes
	 */
	public abstract String readUTFFully();

	/**
	 * Will read bytes from ChannelReader, depending on available(), of a specified length
	 * @param length the number of bytes to read
	 * @return String of UTF bytes
	 */
	public abstract String readUTFOfLength(int length);

	/**
	 * Reads specified length of bytes from ChannelReader depending on available() and, assuming they are UTF, passes them to target
	 * @param length the number of bytes to read
	 * @param target receiver of converted bytes
	 * @return target of specified length with UTF bytes
	 */
	public abstract <A extends Appendable> A readUTFOfLength(int length, A target);

	public abstract long parse(TrieParserReader reader, TrieParser trie, int length);

	/**
	 * Reads data from ChannelReader and checks if the UTF text matches input text
	 * @param equalText text used for comparison
	 * @return boolean saying if data from ChannelReader matches equalText
	 */
	public abstract boolean equalUTF(byte[] equalText);

	/**
	 * Reads UTF bytes from ChannelReader
	 * @return String of UTF bytes
	 */
	public abstract String readUTF();

	/**
	 * Checks to see if ChannelReader has remaining bytes
	 * @return <code>true</code> if there are bytes remaining else <code>false</code>
	 */
	public abstract boolean hasRemainingBytes();
	
	public abstract int read(byte b[]);
	
	public abstract int read(byte b[], int off, int len);

	/**
	 * Checks to see if bytes passed are equal to bytes in ChannelReader
	 * @param bytes byte array to pass in for comparison
	 * @return <code>true</code> if bytes are equal else <code>false</code>
	 */
	public abstract boolean equalBytes(byte[] bytes);

	public abstract boolean equalBytes(byte[] bytes, int bytesPos, int bytesLen);

	/**
	 * Reads data from ChannelReader and passes it to target
	 * @param target to receive data from ChannelReader
	 */
	public abstract void readInto(Externalizable target);

	public abstract void readInto(ChannelWriter writer, int length);

	/**
	 * Reads data from ChannelReader and passes it to Object
	 * @return Object with data from ChannelReader
	 */
	public abstract Object readObject();

	/**
	 * Reads packed Chars from ChannelReader and passes them to target
	 * @param target to receive packed Chars
	 * @return <code>target</code>
	 */
	public abstract <A extends Appendable> A readPackedChars(A target);

	/**
	 * Reads data from ChannelReader to check if data was packed null
	 * @return <code>true</code> if data was packed null
	 */
	public abstract boolean wasPackedNull();

	/**
	 * Reads data from ChannelReader to check if data was decimal null
	 * @return <code>true</code> if data was decimal null
	 */
	public abstract boolean wasDecimalNull();

	/**
	 * Reads packed Long from ChannelReader and passes as long
	 * @return long with data from ChannelReader
	 */
	public abstract long readPackedLong();

	/**
	 * Reads packed Int from ChannelReader and passes to int
	 * @return int with data from ChannelReader
	 */
	public abstract int readPackedInt();

	public abstract double readDecimalAsDouble();
	
	public abstract double readRationalAsDouble();	
	
	public abstract long readDecimalAsLong();

	/**
	 * Reads packed Short from ChannelReader and passes as short
	 * @return short with data from ChannelReader
	 */
	public abstract short readPackedShort();

	/**
	 * Reads Byte from ChannelReader and passes as byte
	 * @return byte with data from ChannelReader
	 */
	public abstract byte readByte();

	/**
	 * Reads Short from ChannelReader and passes as short
	 * @return short with data from ChannelReader
	 */
	public abstract short readShort();

	/**
	 * Reads Int from ChannelReader and passes as int
	 * @return in with data from ChannelReader
	 */
	public abstract int readInt();

	/**
	 * Reads Long from ChannelReader and passes as long
	 * @return long with data from ChannelReader
	 */
	public abstract long readLong();

	/**
	 * Reads Double from ChannelReader and passes as double
	 * @return double with data from ChannelReader
	 */
	public abstract double readDouble();

	/**
	 * Reads Float from ChannelReader and passes as float
	 * @return float with data from ChannelReader
	 */
	public abstract float readFloat();

	/**
	 * Reads data from ChannelReader and passes as int
	 * @return int with data from ChannelReader
	 */
	public abstract int read();

	/**
	 * Reads Boolean from ChannelReader and passes as boolean
	 * @return boolean with data from ChannelReader
	 */
	public abstract boolean readBoolean();

	/**
	 * Reads data from ChannelReader and checks if null, if not passes bool as boolean
	 * @return boolean with data from ChannelReader
	 */
	public abstract boolean wasBooleanNull();

	/**
	 * Reads data from ChannelReader after skipping n bytes and passes data as int
	 * @param n number of bytes to skip
	 * @return int with data from ChannelReader
	 */
	public abstract int skipBytes(int n);
	
	public abstract int absolutePosition();
	
	public abstract void absolutePosition(int position);
	
	public abstract int position();

	public abstract void position(int position);
	
	public abstract boolean isStructured();

	public abstract StructuredReader structured();
	
}
