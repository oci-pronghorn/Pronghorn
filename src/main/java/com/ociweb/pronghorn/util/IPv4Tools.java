package com.ociweb.pronghorn.util;

import java.net.InetAddress;
import java.util.List;

public class IPv4Tools {

	public static TrieParser addressParser = buildAddressParser();
	
	public static TrieParser buildAddressParser() {
		
		TrieParser parser = new TrieParser(1,false);
		
		parser.setUTF8Value("%i.%i.%i.%i", 4);
		parser.setUTF8Value("%i.%i.%i.*",  3);
		parser.setUTF8Value("%i.%i.*.*",   2);
		parser.setUTF8Value("%i.*.*.*",    1);
		parser.setUTF8Value("*.*.*.*",     0);
		
		return parser;
	}


	public static String patternMatchHost(TrieParserReader reader, int token, List<InetAddress> addrList) {
		String bindHost;
		if (addrList.isEmpty()) {
			bindHost = "127.0.0.1";
		} else {
			
			InetAddress selected = null;
			if (token>=0) {
				
				for(int i=0; i<addrList.size(); i++) {
					InetAddress item = addrList.get(i);
					byte[] addr = item.getAddress();
					
					boolean matches = true;//will match if token is zero.
					for(int j=0; j<token; j++) {
						matches &= (((byte)reader.capturedLongField(reader, j)) == addr[0]);
					}
					
					if (matches) {
						selected = item;
						break;
					}
				}
									
				if (null==selected) {
					selected = IPv4Tools.selectExternalAddress(addrList);
				}
				
			} else {
				selected = IPv4Tools.selectExternalAddress(addrList);
			}
			
			bindHost = selected.toString().replace("/", "");
		}
		return bindHost;
	}

	public static InetAddress selectExternalAddress(List<InetAddress> addrList) {
		//skip all 10. and 192.168 and 172.16 so first looking for an external.
		int i = addrList.size();
		while (--i>=0) {
			InetAddress address = addrList.get(i);
			byte[] bytes = address.getAddress();
			if (   (!(bytes[0]==10)) 
				&& (!(bytes[0]==192 && bytes[1]==168)) 
				&& (!(bytes[0]==172 && bytes[1]==16)) )  {
				return address;
			}
		}
		//if external is not found then pick the first one.
		return addrList.get(0);
	}

}
