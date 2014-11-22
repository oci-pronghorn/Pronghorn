package com.ociweb.jfast.field;

import static org.junit.Assert.*;

import org.junit.Test;

import com.ociweb.jfast.catalog.loader.TemplateCatalogConfig;
import com.ociweb.pronghorn.ring.token.TokenBuilder;

public class FieldReaderIntegerTest {

	@Test
	public void testAbsentValue00() {
		assertEquals(1,TokenBuilder.absentValue32(0));		
	}
	
	@Test
	public void testAbsentValue01() {
		assertEquals(0,TokenBuilder.absentValue32(1));		
	}
	
	@Test
	public void testAbsentValue10() {
		assertEquals(-1,TokenBuilder.absentValue32(2));		
	}
	
	@Test
	public void testAbsentValue11() {
		assertEquals(TemplateCatalogConfig.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT,TokenBuilder.absentValue32(3));		
	}
}
