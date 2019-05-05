package com.ociweb.pronghorn.util;

import org.junit.Test;

public class TrieParserCompilerTest {

	
	private TrieParser buildExample() {
		
		TrieParser map = new TrieParser(512,2,false //never skip deep check so we can return 404 for all "unknowns"
				 ,true, //supports extraction
				 true); //ignore case

		map.setUTF8Value("%b ",67108863);
		map.setUTF8Value("/plaintext ",0);
		map.setUTF8Value("/json ",1);
		map.setUTF8Value("/db ",2);
		map.setUTF8Value("/queries?queries=%i ",3);
		map.setUTF8Value("/queries",4);//removed space to ensure this is subset of the above
		map.setUTF8Value("/queries?queries=%b ",5);
		map.setUTF8Value("/updates?queries=%i ",6);
		map.setUTF8Value("/updates",7);//removed space to ensure this is subset of the above
		map.setUTF8Value("/updates?queries=%b ",8);
		map.setUTF8Value("/fortunes ",9);
		
		return map;
	}
	
	@Test
	public void simpleTest() {
		
		String result = TrieParserCompiler.genSource(buildExample(),-1,-2);
		
		
		System.out.println(result);
		
	}
	
	
}
