package com.ociweb.pronghorn.ring.util.build;

import java.lang.reflect.Field;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.schema.loader.TemplateHandler;

public class FROMValidation {

	public static boolean testForMatchingFROMs(String templateFile, String varName, FieldReferenceOffsetManager encodedFrom) {
		try {
			FieldReferenceOffsetManager expectedFrom = TemplateHandler.loadFrom(templateFile);
			if (null==expectedFrom) {
			    System.err.println("Unable to find: "+templateFile);
			    return false;
			}
			if (!expectedFrom.equals(encodedFrom)) {
				System.err.println("Encoded source:"+expectedFrom);
				System.err.println("Template file:"+encodedFrom);
				
				System.err.println("//replacement source");
				StringBuilder target = new StringBuilder();
				TemplateHandler.buildFROMConstructionSource(target, expectedFrom, varName, templateFile.substring(1+templateFile.lastIndexOf('/') ));												
				System.err.println(target);

				return false;
			}			
			
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	/**
	 * Confirm test target has all the right constants if not export the right source code.
	 * @param testTarget
	 * @param encodedFrom
	 * @return
	 */
	public static boolean testForMatchingLocators(Class testTarget, FieldReferenceOffsetManager encodedFrom) {
	    
	    Field[] fields = testTarget.getFields();
	    
	    //confirm that every field accessor is found
	    //if not report single error and full source as it should be.
	    
	    
	    
	    
	    
	    
	    return true;
	}

	
	//TODO: we need something similar for the Twitter App Id work, we can save it as part of the unit tests and generate a deployable which is obfuscated.
	
}
