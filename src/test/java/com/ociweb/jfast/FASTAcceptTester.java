package com.ociweb.jfast;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.Arrays;
import java.util.concurrent.Callable;

import com.ociweb.jfast.write.WriteGroup;

public class FASTAcceptTester implements FASTAccept {

	FASTProvide provider;
		
	StringBuilder builder = new StringBuilder();
	int lastBuilderColumn=-1;
	DecimalDTO decimal = new DecimalDTO();
	
	public FASTAcceptTester(Callable<FASTProvide> c) {
		try {
			this.provider = c.call();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
				
	public void accept(int id, long value) {		
		//MUST test like this or we end up with the cost of boxing the int.
		assertTrue(provider.provideLong(id)==value);
	};
	public void accept(int id, int value) {
		//MUST test like this or we end up with the cost of boxing the int.
		if (provider.provideInt(id) != value) {
			assertEquals(provider.provideInt(id),value);
		}
	};
	public void accept(int id, int exponent, long mantissa)	{
		
		provider.provideDecimal(id, decimal);
		assertEquals(decimal.exponent,exponent);
		assertEquals(decimal.mantissa,mantissa);
		
	};
	
	public void accept(int id, BytesSequence value)	{
		byte[] expected = provider.provideBytes(id);
		assertTrue(value.isEqual(expected));
	}

	public void accept(int id, CharSequence value) {
		assertFalse(provider.provideNull(id));
		
		CharSequence expected = provider.provideCharSequence(id);
		int i = expected.length();
		//System.err.println(i+" "+value.length());
		assertTrue(i==value.length());
		
		while (--i>=0) {
			assertTrue(expected.charAt(i)==value.charAt(i));
		}
	}

	public void accept(int id) {
		assertTrue(provider.provideNull(id));
	}


}
