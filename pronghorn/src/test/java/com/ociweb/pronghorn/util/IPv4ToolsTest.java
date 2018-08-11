package com.ociweb.pronghorn.util;

import static org.junit.Assert.*;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

public class IPv4ToolsTest {

	
	@Test
	public void matchTest() {
		
		TrieParserReader reader = new TrieParserReader(true);
		
		assertEquals(4, reader.query(IPv4Tools.addressParser, "1.2.3.4"));
		assertEquals(3, reader.query(IPv4Tools.addressParser, "5.6.7.*"));
		assertEquals(2, reader.query(IPv4Tools.addressParser, "8.9.*.*"));
		assertEquals(1, reader.query(IPv4Tools.addressParser, "10.*.*.*"));
		assertEquals(0, reader.query(IPv4Tools.addressParser, "*.*.*.*"));		
		
		
		int token = (int)reader.query(IPv4Tools.addressParser, "8.9.*.*");
		List<InetAddress> addrList = new ArrayList<InetAddress>();
		
		try {
			addrList.add(InetAddress.getByAddress("aaa",new byte[]{(byte)1,(byte)2,(byte)3,(byte)4}));
			addrList.add(InetAddress.getByAddress("bbb",new byte[]{(byte)8,(byte)9,(byte)3,(byte)4}));
			addrList.add(InetAddress.getByAddress("ccc",new byte[]{(byte)10,(byte)2,(byte)3,(byte)4}));
			
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
				
		String host = IPv4Tools.patternMatchHost(reader, token, addrList);
		assertEquals("bbb8.9.3.4",host);
				
	}
	
	
}
