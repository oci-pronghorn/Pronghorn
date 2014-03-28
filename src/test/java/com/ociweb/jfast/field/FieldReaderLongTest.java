package com.ociweb.jfast.field;

import static org.junit.Assert.*;

import org.junit.Test;

import com.ociweb.jfast.loader.TemplateCatalog;

public class FieldReaderLongTest {

	@Test
	public void testAbsentValue00() {
		assertEquals(1,FieldReaderLong.absentValue(0));		
	}
	
	@Test
	public void testAbsentValue01() {
		assertEquals(0,FieldReaderLong.absentValue(1));		
	}
	
	@Test
	public void testAbsentValue10() {
		assertEquals(-1,FieldReaderLong.absentValue(2));		
	}
	
	@Test
	public void testAbsentValue11() {
		assertEquals(TemplateCatalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_LONG,FieldReaderLong.absentValue(3));		
	}
	
	@Test
	public void testAbsentValueDefault() {
		assertEquals(3,TokenBuilder.MASK_ABSENT_DEFAULT);
	}
}
