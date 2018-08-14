package com.ociweb.pronghorn.pipe;

public interface TextReader {

	/**
	 * Reads bytes from ChannelReader depending on leading short for length
	 * assuming bytes are UTF-8 encoded passes them to target
	 * @param target receiver of converted bytes
	 * @return target with UTF bytes
	 */
	public <A extends Appendable> A readUTF(A target);
	
	/**
	 * Reads UTF bytes from ChannelReader
	 * @return String of UTF bytes
	 */
	public String readUTF();
	
	/**
	 * Reads data from ChannelReader and checks if the UTF text matches input text
	 * @param equalText text used for comparison
	 * @return boolean saying if data from ChannelReader matches equalText
	 */
	public boolean equalUTF(byte[] equalText);
	
	
}
