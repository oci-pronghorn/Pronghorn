package com.ociweb.pronghorn.stage.monitor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.schema.loader.TemplateHandler;

public class MonitorFROMTest {
	
	@Test
	public void testThis() {
		String templateFile = "/ringMonitor.xml";
		String varName = "monitorFROM";	
		testForMatchingFROMs(templateFile, varName, MonitorFROM.FROM); 
	};
	
	
	public static void testForMatchingFROMs(String templateFile, String varName, FieldReferenceOffsetManager encodedFrom) {
		try {
			FieldReferenceOffsetManager expectedFrom = TemplateHandler.loadFrom(templateFile);
			assertNotNull(expectedFrom);				
			
			if (!expectedFrom.equals(encodedFrom)) {
				System.err.println("Encoded source:"+expectedFrom);
				System.err.println("Template file:"+encodedFrom);	
				
				assertEquals("Scripts same length",expectedFrom.tokens.length,expectedFrom.fieldNameScript.length);
				
				StringBuilder target = new StringBuilder();
				TemplateHandler.buildFROMConstructionSource(target, expectedFrom, varName, templateFile.substring(1+templateFile.lastIndexOf('/') ));												

				if (null!=encodedFrom) {
					assertEquals("FieldNameScript "+target, expectedFrom.fieldNameScript.length,encodedFrom.fieldNameScript.length);
					assertEquals("FieldIdScript"+target,expectedFrom.fieldIdScript.length,encodedFrom.fieldIdScript.length);
					assertEquals("DictionaryNameScript"+target,expectedFrom.dictionaryNameScript.length,encodedFrom.dictionaryNameScript.length);
				}
				
				System.err.println(target);
				
				fail(varName+" template does not match template file "+templateFile);
			}			
			
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
