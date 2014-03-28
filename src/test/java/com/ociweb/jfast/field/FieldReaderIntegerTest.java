package com.ociweb.jfast.field;

import static org.junit.Assert.*;

import org.junit.Test;

import com.ociweb.jfast.loader.TemplateCatalog;

public class FieldReaderIntegerTest {

	@Test
	public void testAbsentValue00() {
		assertEquals(1,FieldReaderInteger.absentValue(0));		
	}
	
	@Test
	public void testAbsentValue01() {
		assertEquals(0,FieldReaderInteger.absentValue(1));		
	}
	
	@Test
	public void testAbsentValue10() {
		assertEquals(-1,FieldReaderInteger.absentValue(2));		
	}
	
	@Test
	public void testAbsentValue11() {
		assertEquals(TemplateCatalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT,FieldReaderInteger.absentValue(3));		
	}
}
