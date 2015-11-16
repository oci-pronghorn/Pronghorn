package com.ociweb.pronghorn.pipe.util.build;

import java.lang.reflect.Field;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.pipe.schema.loader.TemplateHandler;
import com.ociweb.pronghorn.pipe.token.TokenBuilder;

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
	public static <S extends MessageSchema> boolean testForMatchingLocators(S schema) {
	    
	    FieldReferenceOffsetManager encodedFrom = MessageSchema.from(schema);
	    
	    Field[] fields = schema.getClass().getFields();
	    
	    int[] msgStart = encodedFrom.messageStarts;
	    
	    StringBuilder generatedSource = new StringBuilder();
	    
	    
	    boolean success = true;
	    for(int i = 0 ; i<msgStart.length; i++) {
	        
	        int expectedMsgIdx = msgStart[i];
	        String name = encodedFrom.fieldNameScript[expectedMsgIdx];	
	        //only generate constatns for named fields.
	        if (null!=name) {
    	        long id = encodedFrom.fieldIdScript[expectedMsgIdx];
    	        
    	        String messageConstantName = "MSG_"+name.toUpperCase().replace(' ','_')+"_"+id;
    	        
    	        appendAssignmentCode(generatedSource, messageConstantName, expectedMsgIdx);
    	            	        
    	        boolean found = false;
    	        int j = fields.length;
    	        while (--j>=0 && !found) {
    	            String schemaFieldName = fields[j].getName();
    	            if (schemaFieldName.equals(messageConstantName)) {
    	                found = true;    	                
    	                try {
                            int assignedValue = fields[j].getInt(null);
                            if (expectedMsgIdx != assignedValue) {
                                success = false;
                                System.err.println("//wrong expected value: "+messageConstantName);
                            }                            
    	                } catch (IllegalArgumentException e) {                           
                            e.printStackTrace();
                            found = false;
                        } catch (IllegalAccessException e) {                            
                            e.printStackTrace();
                            found = false;
                        }
    	            }
    	        }
    	        if (!found) {
    	            success = false;
    	            System.err.println("//unable to find: "+messageConstantName);
    	        }
    	        
    	        
    	        int fieldLimit;
    	        if (i+1>=msgStart.length) {
    	            fieldLimit = encodedFrom.fieldIdScript.length;
    	        } else {
    	            fieldLimit = msgStart[i+1];
    	        }
    	            
    	        
    	        for(int fieldIdx = msgStart[i]+1; fieldIdx<fieldLimit; fieldIdx++) {
    	            String msgFieldName = encodedFrom.fieldNameScript[fieldIdx]; 
    	            if (null!=msgFieldName) {
    	                long imsgFieldId = encodedFrom.fieldIdScript[fieldIdx];
    	                
    	                
    	                int fieldLOC = FieldReferenceOffsetManager.paranoidLookupFieldLocator(imsgFieldId, msgFieldName, expectedMsgIdx, encodedFrom);
    	                
    	                //TODO: if two fields are the same need to build a single constant that can be used for either
    	                //      check if fieldLoc matches and fieldname and fieldid all match
                        //       
    	                    	                
    	                String messageFieldConstantName = messageConstantName+"_FIELD_"+msgFieldName.toUpperCase().replace(' ','_')+"_"+imsgFieldId;    	                
    	                appendAssignmentCode(generatedSource, messageFieldConstantName, fieldLOC);
    	        
    	                found = false;
    	                j = fields.length;
    	                while (--j>=0 && !found) {
    	                    String schemaFieldName = fields[j].getName();
    	                    if (schemaFieldName.equals(messageFieldConstantName)) {
    	                        found = true;                       
    	                        try {
    	                            int assignedValue = fields[j].getInt(null);
    	                            if (fieldLOC != assignedValue) {
    	                                success = false;
    	                                System.err.println("//wrong expected value: "+messageFieldConstantName);
    	                            }                            
    	                        } catch (IllegalArgumentException e) {                           
    	                            e.printStackTrace();
    	                            found = false;
    	                        } catch (IllegalAccessException e) {                            
    	                            e.printStackTrace();
    	                            found = false;
    	                        }
    	                    }
    	                }
    	                if (!found) {
    	                    success = false;
    	                    System.err.println("//unable to find: "+messageFieldConstantName);
    	                }    	                
    	               
    	            }
    	        }
    	        
	        
	        }
	    }
	    
	    if (!success) {
	        System.err.println(generatedSource);
	    }
	    return success;
	}

    private static void appendAssignmentCode(StringBuilder result, String constantName, int value) {
       
        result.append("public static final int ").append(constantName).append(" = 0x").append(Integer.toHexString(value)).append(";\n");
        
    }

}
