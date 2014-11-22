package com.ociweb.jfast.field;

import static org.junit.Assert.*;

import org.junit.Test;

import com.ociweb.jfast.catalog.loader.TemplateCatalogConfig;
import com.ociweb.pronghorn.ring.token.TokenBuilder;

public class FieldReaderLongTest {

	@Test
	public void testAbsentValue00() {
		assertEquals(1,TokenBuilder.absentValue64(0));		
	}
	
	@Test
	public void testAbsentValue01() {
		assertEquals(0,TokenBuilder.absentValue64(1));		
	}
	
	@Test
	public void testAbsentValue10() {
		assertEquals(-1,TokenBuilder.absentValue64(2));		
	}
	
	@Test
	public void testAbsentValue11() {
		assertEquals(TemplateCatalogConfig.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_LONG,TokenBuilder.absentValue64(3));		
	}
	
	@Test
	public void testAbsentValueDefault() {
		assertEquals(3,TokenBuilder.MASK_ABSENT_DEFAULT);
	}
}
