package com.ociweb.pronghorn.pipe;

import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;

import java.io.Externalizable;
import java.io.InputStream;
import java.io.ObjectInput;

public abstract class ChannelReader extends InputStream implements ObjectInput, TextReader {

	//Max bytes consumed by a packed integer
	public static final int PACKED_INT_SIZE = 5;
	
	//Max bytes consumed by a packed long
	public static final int PACKED_LONG_SIZE = 10;
		
	public static final int BOOLEAN_SIZE = 1;
	
	/**
	 * Will give how many bytes are available right now
	 * @return the number of bytes
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
	 * Reads all UTF bytes from ChannelReader, does not use leading short
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
	 * @param <A> type of appendable target
	 * @return target of specified length with UTF bytes
	 */
	public abstract <A extends Appendable> A readUTFOfLength(int length, A target);

	/**
	 * Parses data in reader and returns numeric representation of trie
	 * @param reader source data to parse from
	 * @param trie rules for parsing
	 * @param length max value to parse
	 * @return long value from trie, -1 if not found
	 */
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

	/**
	 * Reads bytes from ChannelReader and puts them in b[]
	 * @param b array to store bytes read
	 * @return number of bytes stored
	 */
	public abstract int read(byte b[]);

	/**
	 * Reads bytes from ChannelReader and puts them in b[]
	 * @param b array to store bytes read
	 * @param off offset to start reading
	 * @param len length of bytes to write
	 * @return number of bytes stored
	 */
	public abstract int read(byte b[], int off, int len);

	/**
	 * Checks to see if bytes passed are equal to bytes in ChannelReader
	 * @param bytes byte array to pass in for comparison
	 * @return <code>true</code> if bytes are equal else <code>false</code>
	 */
	public abstract boolean equalBytes(byte[] bytes);

	/**
	 * Checks to see if bytes passed are equal to bytes in ChannelReader
	 * @param bytes byte array to pass in for comparison
	 * @param bytesPos position to read from
	 * @param bytesLen length of bytes to read
	 * @return <code>true</code> if bytes are equal else <code>false</code>
	 */
	public abstract boolean equalBytes(byte[] bytes, int bytesPos, int bytesLen);

	/**
	 * Reads data from ChannelReader and passes it to target
	 * @param target to receive data from ChannelReader
	 */
	public abstract void readInto(Externalizable target);

	public abstract void readInto(ChannelWriter writer, int length);
	
	public abstract int readInto(byte[] b, int off, int len, int mask);
	 

	/**
	 * Reads data from ChannelReader and passes it to Object
	 * @return Object with data from ChannelReader
	 */
	public abstract Object readObject();

	/**
	 * Reads packed Chars from ChannelReader and passes them to target
	 * @param target to receive packed Chars
	 * @param <A> type of appendable target
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
	 * Reads packed Long from ChannelReader and passes to long
	 * @return long with data from ChannelReader
	 */
	public abstract long readPackedLong();

	/**
	 * Reads packed Int from ChannelReader and passes to int
	 * @return int with data from ChannelReader
	 */
	public abstract int readPackedInt();

	/**
	 * Reads decimal from ChannelReader and passes to double
	 * @return double with data from ChannelReader
	 */
	public abstract double readDecimalAsDouble();

    /**
     * Reads decimal from ChannelReader and writes to target
     * @param target Appendable target
     * @param <A> type of appendable target
     * @return target Appendable
     */
	public abstract <A extends Appendable> A readDecimalAsText(A target);
	
	/**
	 * Reads rational from ChannelReader and passes to double
	 * @return double with data from ChannelReader
	 */
	public abstract double readRationalAsDouble();
	
	
	/**
	 * Reads rational from ChannelReader and writes it as text to target
	 * @param target Appendable target
     * @param <A> type of appendable target
	 * @return target Appendable
	 */
	public abstract <A extends Appendable> A readRationalAsText(A target);

	/**
	 * Reads decimal from ChannelReader and passes to long
	 * @return long with data from ChannelReader
	 */
	public abstract long readDecimalAsLong();

	/**
	 * Reads packed Short from ChannelReader
	 * @return short with data from ChannelReader
	 */
	public abstract short readPackedShort();

	/**
	 * Reads Byte from ChannelReader
	 * @return byte with data from ChannelReader
	 */
	public abstract byte readByte();

	/**
	 * Reads Short from ChannelReader
	 * @return short with data from ChannelReader
	 */
	public abstract short readShort();

	/**
	 * Reads Int from ChannelReader
	 * @return int with data from ChannelReader
	 */
	public abstract int readInt();

	/**
	 * Reads Long from ChannelReader
	 * @return long with data from ChannelReader
	 */
	public abstract long readLong();

	/**
	 * Reads Double from ChannelReader
	 * @return double with data from ChannelReader
	 */
	public abstract double readDouble();

	/**
	 * Reads Float from ChannelReader
	 * @return float with data from ChannelReader
	 */
	public abstract float readFloat();

	/**
	 * Reads data from ChannelReader
	 * @return int with data from ChannelReader
	 */
	public abstract int read();

	/**
	 * Reads Boolean from ChannelReader
	 * @return boolean with data from ChannelReader
	 */
	public abstract boolean readBoolean();

	/**
	 * Reads data from ChannelReader and checks if null, if not passes as boolean
	 * @return boolean with data from ChannelReader
	 */
	public abstract boolean wasBooleanNull();

	/**
	 * Reads data from ChannelReader after skipping n bytes and passes data as int
	 * @param n number of bytes to skip
	 * @return int with data from ChannelReader
	 */
	public abstract int skipBytes(int n);

	/**
	 * @return actual offset position in the array where you are right now
	 */
	public abstract int absolutePosition();

	/**
	 * @param position the actual offset position in the array where you want to be right now
	 */
	public abstract void absolutePosition(int position);

	/**
	 * @return your position relative the the absolute position (if you've read 3 bytes since the absolutePosition, your position is 3)
	 */
	public abstract int position();

	/**
	 * @param position offset that you want from the absolutePosition
	 */
	public abstract void position(int position);

	/**
	 * Determines if ChannelReader is structured
	 * @return <code>true</code> if ChannelReader is structured else <code>false</code>
	 */
	public abstract boolean isStructured();

	/**
	 * Structures ChannelReader allowing access to field name
	 * @return structured form of ChannelReader
	 */
	public abstract StructuredReader structured();
	
}
