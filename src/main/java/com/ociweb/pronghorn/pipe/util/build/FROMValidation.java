package com.ociweb.pronghorn.pipe.util.build;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

import javax.xml.parsers.ParserConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.pipe.schema.loader.TemplateHandler;
import com.ociweb.pronghorn.pipe.token.TokenBuilder;
import com.ociweb.pronghorn.pipe.token.TypeMask;
import com.ociweb.pronghorn.util.Appendables;

public class FROMValidation {

	public final static Logger logger = LoggerFactory.getLogger(FROMValidation.class);
	
	public static boolean forceCodeGen = false;
	
    public static <S extends MessageSchema<S>> boolean testForMatchingFROMs(String templateFile, S schema) {
        try {
            FieldReferenceOffsetManager encodedFrom = null;
            try {
                encodedFrom = MessageSchema.from(schema); //TODO: new projects get null pointer here, fix so they are given correct source.
            } catch (NullPointerException npe) {
                //continue with no previous FROM
            }
            return testForMatchingFROMs(templateFile, encodedFrom);           
            
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

    }


	private static boolean testForMatchingFROMs(String templateFile, FieldReferenceOffsetManager encodedFrom)
			throws ParserConfigurationException, SAXException, IOException {
		FieldReferenceOffsetManager expectedFrom = TemplateHandler.loadFrom(templateFile);
		if (null==expectedFrom) {
		    logger.error("Unable to find: "+templateFile);
		    return false;
		}
		if (null==encodedFrom || !expectedFrom.equals(encodedFrom)) {
		    logger.error("Encoded source:"+expectedFrom);
		    if (null!=encodedFrom) {
		        logger.error("Template file:"+encodedFrom);
		    }
		    logger.error("//replacement source");
		    StringBuilder target = new StringBuilder();
		    String nameOfFROM = templateFile.substring(1+templateFile.lastIndexOf('/') );
		    
		    FieldReferenceOffsetManager.buildFROMConstructionSource(target, expectedFrom, "FROM", nameOfFROM);  
		    

		    System.out.println();
		    System.out.println(target.toString());
		    
		    
		    return false;
		}
		return true;
	}
    
	public static <S extends MessageSchema<S>> boolean checkSchema(String templateFile, Class<S> clazz, boolean forceCode) {
		try {
			FROMValidation.forceCodeGen = forceCode;		
			return checkSchema(templateFile, clazz);
		} finally {
			FROMValidation.forceCodeGen = false;
		}
	}
	
	public static <S extends MessageSchema<S>> boolean checkSchema(String templateFile, Class<S> clazz) {
		if (!testForMatchingFROMs(templateFile, clazz)) {
			return false;
		}		
		return testForMatchingLocators(clazz);
	}
	
    public static <S extends MessageSchema<S>> boolean testForMatchingFROMs(String templateFile, Class<S> clazz) {
    	
    	S found = MessageSchema.findInstance(clazz);
    	if (null!=found) {
    		return testForMatchingFROMs(templateFile, found);
    	} else {
    		try {
				boolean result = testForMatchingFROMs(templateFile, (FieldReferenceOffsetManager)null);
				
			    //public static final RawDataSchema instance = new RawDataSchema();
			    if (!result) {
			    	//show the new constructor
			    	System.out.println();
			    	System.out.println("protected "+clazz.getSimpleName()+"() { ");
			    	System.out.println("    super(FROM);");
			    	System.out.println("}");
			    	System.out.println();
			    	//show the line needed for adding the instance
			    	System.out.println("public static final "+clazz.getSimpleName()+" instance = new "+clazz.getSimpleName()+"();");
			    }
				
				return result;
			} catch (Exception e) {
				logger.error("unable to build FROM {} {}",e.getClass().getSimpleName(),e.getMessage());
				return false;
			}
    	}
    	
    }
    
	/**
	 * Confirm test target has all the right constants if not export the right source code.
	 */
    public static <S extends MessageSchema<S>> boolean testForMatchingLocators(Class<S> clazz) {

    	S found = MessageSchema.findInstance(clazz);
    	if (null!=found) {
    		return testForMatchingLocators(found);
    	}
    	return false;
    }


	/**
	 * Confirm test target has all the right constants if not export the right source code.
	 */
	public static <S extends MessageSchema> boolean testForMatchingLocators(S schema) {

	    FieldReferenceOffsetManager encodedFrom = MessageSchema.from(schema);
	    
	    Field[] fields = schema.getClass().getFields();
	    
	    if (MessageSchema.class != schema.getClass().getSuperclass()) {
	        System.out.println("all Schema objects must directly extend "+MessageSchema.class.getCanonicalName());
	        return false;
	    }
	    
	    int[] msgStart = encodedFrom.messageStarts;
	    
	    //TODO: at some point we want to code generate low level examples...
	    
	    StringBuilder generatedConstants = new StringBuilder();
	    StringBuilder generatedSwitch = new StringBuilder();
	    StringBuilder generatedConsumers = new StringBuilder();
	    
	    StringBuilder generatedProducersTemp1 = new StringBuilder();
	    StringBuilder generatedProducersTemp2 = new StringBuilder();
	    
	    StringBuilder generatedProducers = new StringBuilder();
	    
	    
	    boolean generateExampleMethods = encodedFrom.hasSimpleMessagesOnly;
	    
	    if (generateExampleMethods) {
	    	generatedSwitch.append("public static void consume(Pipe<").append(schema.getClass().getSimpleName()).append("> input) {\n");
	    	generatedSwitch.append("    while (PipeReader.tryReadFragment(input)) {\n");
	    	generatedSwitch.append("        int msgIdx = PipeReader.getMsgIdx(input);\n");
	    	generatedSwitch.append("        switch(msgIdx) {\n");	    	
	    }
	    
	    boolean success = true;
	    for(int i = 0 ; i<msgStart.length; i++) {
	        
	        int expectedMsgIdx = msgStart[i];
	        String methodName="unknown";
	        String name = encodedFrom.fieldNameScript[expectedMsgIdx];	
	        //only generate constatns for named fields.
	        if (null!=name) {
	            
    	        String messageConstantName = FieldReferenceOffsetManager.buildMsgConstName(encodedFrom, expectedMsgIdx);
    	        
    	        
    	        		
    	        
    	        appendAssignmentCode(generatedConstants, messageConstantName, expectedMsgIdx, TokenBuilder.tokenToString(encodedFrom.tokens[expectedMsgIdx]));
    	        if (generateExampleMethods) {
    	        	methodName = FieldReferenceOffsetManager.buildName(encodedFrom, expectedMsgIdx);
    	        	appendSwitchCase(generatedSwitch, messageConstantName, methodName);
    	        	appendConsumeMethodBegin(generatedConsumers, methodName, schema);
    	        	generatedProducersTemp1.setLength(0);
    	        	generatedProducersTemp2.setLength(0);
    	        }
    	        
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
                                //logger.error(("//wrong expected value: "+messageConstantName);
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
    	            //logger.error(("//unable to find: "+messageConstantName);
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
    	                appendAssignmentCode(generatedConstants, messageFieldConstantName, fieldLOC, TokenBuilder.tokenToString(encodedFrom.tokens[fieldIdx]));
    	        
    	    	        if (generateExampleMethods) {
    	    	        	String varName = "field"+(msgFieldName.replace(' ','_'));    	    	        	
    	    	        	int token = encodedFrom.tokens[fieldIdx];
    	    	        	appendConsumeMethodField(generatedConsumers, varName, messageFieldConstantName, token, schema);
    	    	        	appendProduceMethodField(generatedProducersTemp1, generatedProducersTemp2, varName, messageFieldConstantName, token);
    	    	        }
    	                
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
    	                                //logger.error(("//wrong expected value: "+messageFieldConstantName);
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
    	                    //logger.error(("//unable to find: "+messageFieldConstantName);
    	                }    	                
    	               
    	            }
    	        }
    	        if (generateExampleMethods) {
    	        	appendProduceMethodEnd(generatedProducersTemp1, generatedProducersTemp2, generatedProducers, methodName, messageConstantName, schema);
    	        	appendConsumeMethodEnd(generatedConsumers);
    	        }
	        }
	    }
	    
	    if (generateExampleMethods) {
	    	generatedSwitch.append("            case -1:\n");
	    	generatedSwitch.append("               //requestShutdown();\n");
	    	generatedSwitch.append("            break;\n");	    	
	    	generatedSwitch.append("        }\n");
	    	generatedSwitch.append("        PipeReader.releaseReadLock(input);\n    }\n}\n");

	    }
	    
    	success = checkForExampleCode(schema, success, "consume"); //must find at least 1 consume method
    	success = checkForExampleCode(schema, success, "publish"); //must find at least 1 publish method
    	
	    
	    if (!success || forceCodeGen) {
	    	//to console, do not log.
	    	System.out.println(generatedConstants);
	        System.out.println();
	        System.out.println(generatedSwitch);
	        System.out.println(generatedConsumers);
	        System.out.println(generatedProducers);
	    }
	    
	    return success;
	}



	private static <S extends MessageSchema<S>> boolean checkForExampleCode(S schema, boolean success, String startsWith) {
		boolean found = false;
    	for(Method m :schema.getClass().getMethods()) {
    		if (m.getName().startsWith(startsWith)) {
    			found = true;
    		}
    	}
    	if (!found) {
    		success = false;
    	}
		return success;
	}

	private static <S extends MessageSchema<S>> void appendConsumeMethodField(StringBuilder generatedConsumers, String varName, String constName, int token, S schema) {
		
		int type = TokenBuilder.extractType(token);
		
		if (TypeMask.isInt(type)) {
			
			generatedConsumers.append("    int ").append(varName).append(" = PipeReader.readInt(input,").append(constName).append(");\n");
				
		} else if (TypeMask.isLong(type)) {
			
			generatedConsumers.append("    long ").append(varName).append(" = PipeReader.readLong(input,").append(constName).append(");\n");

		} else if (TypeMask.isDecimal(type)) {
			
			generatedConsumers.append("    int ").append(varName).append("e = PipeReader.readInt(input,").append(constName).append(");\n");
			generatedConsumers.append("    long ").append(varName).append("m = PipeReader.readLong(input,").append(constName).append(");\n");

		} else if (TypeMask.isText(type)) {    	    	        		    	    	       		
			
			generatedConsumers.append("    StringBuilder ").append(varName).append(" = PipeReader.readUTF8(input,").append(constName)
													   .append(",new StringBuilder(PipeReader.readBytesLength(input,")
													   .append(constName).append(")));\n");
						
		} else if (TypeMask.isByteVector(type)) {
			generatedConsumers
								.append("    DataInputBlobReader<").append(schema.getClass().getSimpleName()).append("> ")
								.append(varName)
								.append(" = PipeReader.inputStream(input, ")
								.append(constName)
								.append(");\n");
																	     		
		} else {
			throw new UnsupportedOperationException("unknown value "+type);
		}
	}
	
	private static void appendProduceMethodField(StringBuilder argsTemp, StringBuilder bodyTemp, String varName, String constName, int token) {
		
		int type = TokenBuilder.extractType(token);
		
		if (TypeMask.isInt(type)) {
			
			argsTemp.append("int ").append(varName).append(", ");
			bodyTemp.append("        PipeWriter.writeInt(output,").append(constName).append(", ").append(varName).append(");\n");

		} else if (TypeMask.isLong(type)) {
			
			argsTemp.append("long ").append(varName).append(", ");
			bodyTemp.append("        PipeWriter.writeLong(output,").append(constName).append(", ").append(varName).append(");\n");

		} else if (TypeMask.isDecimal(type)) {
			
			argsTemp.append("int ").append(varName).append(", ");
			argsTemp.append("long ").append(varName).append(", ");
			bodyTemp.append("        PipeWriter.writeDecimal(output,").append(constName).append(", ").append(varName).append("e, ").append(varName).append("m);\n");

		} else if (TypeMask.isText(type)) {    	    	        		    	    	       		
			
			argsTemp.append("CharSequence ").append(varName).append(", ");
			bodyTemp.append("        PipeWriter.writeUTF8(output,").append(constName).append(", ").append(varName).append(");\n");
		
		} else if (TypeMask.isByteVector(type)) {
			
			argsTemp.append("byte[] ").append(varName).append("Backing, ");
			argsTemp.append("int ").append(varName).append("Position, ");
			argsTemp.append("int ").append(varName).append("Length, ");
			bodyTemp.append("        PipeWriter.writeBytes(output,").append(constName).append(", ")
			              .append(varName).append("Backing, ").append(varName).append("Position, ").append(varName).append("Length")
			              .append(");\n");
			
		} else {
			throw new UnsupportedOperationException("unknown value "+type);
		}
	}

	
    private static void appendConsumeMethodBegin(StringBuilder generatedConsumers, String methodName, MessageSchema schema ) {
    	generatedConsumers.append("public static void consume").append(methodName).append("(Pipe<").append(schema.getClass().getSimpleName()).append("> input) {\n");
	}

    
    private static void appendConsumeMethodEnd(StringBuilder generatedConsumers) {
    	generatedConsumers.append("}\n");
	}
    
    private static void appendProduceMethodEnd(StringBuilder argsTemp, StringBuilder bodyTemp, StringBuilder generatedProducers, 
    		                                    String methodName, String messageConst, MessageSchema schema) {

    	if (argsTemp.length()>0) {//remove last comma and space if found
    		argsTemp.setLength(argsTemp.length()-2);
    	}
		
		generatedProducers.append("public static void publish").append(methodName).append("(Pipe<").append(schema.getClass().getSimpleName()).append("> output");
		if (argsTemp.length()>0) {
			generatedProducers.append(", ").append(argsTemp);
		}
		generatedProducers.append(") {\n");
		  		
		generatedProducers.append("        PipeWriter.presumeWriteFragment(output, ").append(messageConst).append(");\n");
    	generatedProducers.append(bodyTemp);
    	generatedProducers.append("        PipeWriter.publishWrites(output);\n");

    	generatedProducers.append("}\n");
    	
	}
    
	private static void appendSwitchCase(StringBuilder result, String messageConstantName, String name) {
		result.append("            case ").append(messageConstantName).append(":\n");
		result.append("                consume"+name+"(input);\n");
		result.append("            break;\n");
	}

	private static void appendAssignmentCode(StringBuilder result, String constantName, int value, String comment) {
       
        result.append("public static final int ").append(constantName).append(" = ");
        Appendables.appendFixedHexDigits(result, value, 32).append("; //").append(comment).append("\n");
        
    }

}
