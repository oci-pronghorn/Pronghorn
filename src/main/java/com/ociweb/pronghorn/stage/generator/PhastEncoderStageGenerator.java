package com.ociweb.pronghorn.stage.generator;

import java.io.IOException;
import com.ociweb.pronghorn.stage.phast.PhastEncoder;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;

import static com.ociweb.pronghorn.util.Appendables.*;

import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.token.*;
import com.ociweb.pronghorn.pipe.util.build.TemplateProcessGeneratorLowLevelReader;
import javax.swing.text.html.HTML;
import jdk.nashorn.internal.parser.TokenStream;
import sun.awt.windows.WToolkit;


public class PhastEncoderStageGenerator extends TemplateProcessGeneratorLowLevelReader{

        private final Class encoder = PhastEncoder.class;    
        private final Appendable bodyTarget;        
        private final String defLongDictionaryName = "defLongDictionary";
        private final String defIntDictionaryName = "defIntDictiornary";
        //short not supported yet
        //private final String defShortDictionaryName = "defShortDictiornary";
        private final String longDictionaryName = "previousLongDictionary";
        private final String intDictionaryName = "previousIntDictiornary";
        private final String shortDictionaryName = "previousShortDictiornary";
        private final String writerName = "writer";
        private final String pmapName = "map";
        private final String indexName = "idx";
        private final String bitMaskName = "bitMask";
        private final String intValueName = "intVal";
        private final String longValueName = "longVal";
        private final String shortValueName = "shortVal";
        private final String stringValueName = "stringVal";
        private final String tokenName = "token";
        private final String booleanName = "boolean";
        
    @Override
    public void processSchema() throws IOException{
        super.processSchema();
        additionalImports(schema, bodyTarget);
        additionalTokens(bodyTarget);
        FieldReferenceOffsetManager from = MessageSchema.from(schema);

        bodyBuilder(schema, 0, fragmentParaCount, fragmentParaTypes, fragmentParaArgs,fragmentParaSuff);
    }
    public PhastEncoderStageGenerator(MessageSchema schema, Appendable bodyTarget) {
        super(schema, bodyTarget); 
        this.bodyTarget = bodyTarget;
        }

    @Override
    protected void additionalImports(MessageSchema schema, Appendable target) {
        try {
            target.append("import ").append(schema.getClass().getCanonicalName()).append(";\n");
        } catch (IOException e) {
           throw new RuntimeException(e);
        }
    }
    // Additional Token method to append any longs, ins or string variables
    protected void additionalTokens(Appendable target) throws IOException { 
        FieldReferenceOffsetManager from = MessageSchema.from(schema);
        int[] tokens = from.tokens;
        int[] intDict = from.newIntDefaultsDictionary();
        String[] scriptNames = from.fieldNameScript;
        long[] scriptIds = from.fieldIdScript;
        long[] longDict = from.newLongDefaultsDictionary();
        int i = tokens.length;
        
        while (--i >= 0) {
            int type = TokenBuilder.extractType(tokens[i]);
            
            if (TypeMask.isLong(type)) {                
                target.append("private long ").append(scriptNames[i]).append(";\n");
            }
            else if(TypeMask.isInt(type)) {
                target.append("private int ").append(scriptNames[i]).append(";\n");
            }
            else if(TypeMask.isText(type)) {
                target.append("private String ").append(scriptNames[i]).append(";\n");
            }
        }
    }
    //  BuilderInt Factory
    protected void encodePmapBuilderInt(MessageSchema schema, Appendable target) {
       try {
            appendStaticCall(target, encoder , "pmapBuilderInt(")
                    .append(pmapName).append(", ")
                    .append(tokenName).append(", ")
                    .append(longValueName).append(", ")
                    .append(longValueName).append(", ")
                    .append(longValueName).append(", ")
                    .append(booleanName)
                    .append(");\n");
        } catch (IOException e) {
           throw new RuntimeException(e);
        }
    }
    // builderLong Factory
    protected void encodePmapBuilderLong(MessageSchema schema, Appendable target) {
       try {
            appendStaticCall(target, encoder , "pmapBuilderLong(")
                    .append(pmapName).append(", ")
                    .append(tokenName).append(", ")
                    .append(longValueName).append(", ")
                    .append(longValueName).append(", ")
                    .append(longValueName).append(", ")
                    .append(booleanName)
                    .append(");\n");
        } catch (IOException e) {
           throw new RuntimeException(e);
        }
    }
    // BuilderString Factory
    protected void encodePmapBuilderString(MessageSchema schema, Appendable target) {
       try {
            appendStaticCall(target, encoder , "pmapBuilderString(")
                    .append(pmapName).append(", ")
                    .append(tokenName).append(", ")
                    .append(longValueName).append(", ")
                    .append(longValueName).append(", ")
                    .append(longValueName).append(", ")
                    .append(booleanName)
                    .append(");\n");
        } catch (IOException e) {
           throw new RuntimeException(e);
        }
    }
    // IntPresent Factory
    protected void encodeIntPresentGenerator(MessageSchema schema, Appendable target) {
        try {
            appendStaticCall(target, encoder , "encodeIntPresent(")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName).append(", ")                 
                    .append(intValueName)
                    .append(");\n");
        } catch (IOException e) {
           throw new RuntimeException(e);
        }
    }
    // DeltaInt Factory
    protected void encodeDeltaIntGenerator(MessageSchema schema, Appendable target) {
        try {
            appendStaticCall(target, encoder , "encodeDeltaInt(")
                    .append(intDictionaryName).append(", ")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(indexName).append(", ")                    
                    .append(intValueName)
                    .append(");\n");
        } catch (IOException e) {
           throw new RuntimeException(e);
        }
    }
    // DeltaLong Factory
    protected void encodeDeltaLongGenerator(MessageSchema schema, Appendable target) {
        try {
            appendStaticCall(target, encoder , "encodeDeltaLong(")
                    .append(longDictionaryName).append(", ")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(indexName).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(longValueName)
                    .append(");\n");
        } catch (IOException e) {
           throw new RuntimeException(e);
        }
    }
    // String Factory
    protected void encodeStringGenerator(MessageSchema schema, Appendable target) {
        try {
            appendStaticCall(target, encoder , "encodeString(")
                    .append(writerName).append(", ")
                    .append(writerName).append(", ")
                    .append(stringValueName)
                    .append(");\n");
        } catch (IOException e) {
           throw new RuntimeException(e);
        }
    }
    // IncrementInt Factory
    protected void incrementIntGenerator(MessageSchema schema, Appendable target) {
        try {
            appendStaticCall(target, encoder , "incrementInt(")
                    .append(intDictionaryName).append(", ")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(indexName)
                    .append(");\n");
        } catch (IOException e) {
           throw new RuntimeException(e);
        }    
    }
    // CopyInt Factory
    protected void copyIntGenerator(MessageSchema schema, Appendable target) {
        try {
            appendStaticCall(target, encoder , "copyInt(")
                    .append(intDictionaryName).append(", ")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(indexName).append(", ")
                    .append(intValueName)
                    .append(");\n");
        } catch (IOException e) {
           throw new RuntimeException(e);
        }    
    }
    // DefaultInt Factory
    protected void encodeDefaultIntGenerator(MessageSchema schema, Appendable target) {
        try {
            appendStaticCall(target, encoder , "encodeDefaultInt(")
                    .append(intDictionaryName).append(", ")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(indexName).append(", ")
                    .append(intValueName).append(", ")
                    .append(");\n");
        } catch (IOException e) {
           throw new RuntimeException(e);
        }    
    }
    // LongPresent Factory
    protected void encodeLongPresentGenerator(MessageSchema schema, Appendable target) {
        try {
            appendStaticCall(target, encoder , "encodeLongPresentGenerator(")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(longValueName).append(", ")
                    .append(");\n");
        } catch (IOException e) {
           throw new RuntimeException(e);
        }
    }
    // IncrementLong Factory
    protected void incrementLongGenerator(MessageSchema schema, Appendable target) {
        try {
            appendStaticCall(target, encoder , "incrementLong(")
                    .append(longDictionaryName).append(", ")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(indexName).append(", ")
                    .append(");\n");
        } catch (IOException e) {
           throw new RuntimeException(e);
        }    
    }
    // CopyLong Factory
    protected void copyLongGenerator(MessageSchema schema, Appendable target) {
        try {
            appendStaticCall(target, encoder , "copyLong(")
                    .append(longDictionaryName).append(", ")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(indexName).append(", ")
                    .append(intValueName)
                    .append(");\n");
        } catch (IOException e) {
           throw new RuntimeException(e);
        }   
    }
    // DefaultLong Factory
    protected void encodeDefaultLongGenerator(MessageSchema schema, Appendable target) {
        try {
            appendStaticCall(target, encoder , "encodeDefaultLong(")
                    .append(longDictionaryName).append(", ")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(indexName).append(", ")
                    .append(longValueName).append(", ")
                    .append(");\n");
        } catch (IOException e) {
           throw new RuntimeException(e);
        }   
    }
    // ShortPresent Factory
    protected void encodeShortPresentGenerator(MessageSchema schema, Appendable target) {
        try {
            appendStaticCall(target, encoder , "encodeShortPresent(")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName)
                    .append(shortValueName).append(", ")
                    .append(");\n");
        } catch (IOException e) {
           throw new RuntimeException(e);
        }
    }
    // IncrementShort Factory
    protected void incrementShortGenerator(MessageSchema schema, Appendable target) {
        try {
            appendStaticCall(target, encoder , "incrementShort(")
                    .append(shortDictionaryName).append(", ")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(indexName).append(", ")
                    .append(");\n");
        } catch (IOException e) {
           throw new RuntimeException(e);
        } 
    }
    // CopyShort Factory
    protected void copyShortGenerator(MessageSchema schema, Appendable target) {
        try {
            appendStaticCall(target, encoder , "copyShort")
                    .append(shortDictionaryName).append(", ")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(indexName).append(", ")
                    .append(intValueName)
                    .append(");\n");
        } catch (IOException e) {
           throw new RuntimeException(e);
        }   
    }
    // DefaultShort Factory
    protected void encodeDefaultShortGenerator(MessageSchema schema, Appendable target) {
        try {
            appendStaticCall(target, encoder , "encodeDefaultShort(")
                    .append(shortDictionaryName).append(", ")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName).append(", ")
                    .append(indexName).append(", ")
                    .append(shortValueName).append(", ")
                    .append(");\n");
        } catch (IOException e) {
           throw new RuntimeException(e);
        }   
    }
    // DeltaShort Factory
    protected void encodeDeltaShortGenerator(MessageSchema schema, Appendable target) {
         try {
            appendStaticCall(target, encoder , "encodeDeltaShort(")
                    .append(shortDictionaryName).append(", ")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(indexName).append(", ") 
                    .append(bitMaskName).append(", ")             
                    .append(shortValueName).append(", ")
                    .append(");\n");
        } catch (IOException e) {
           throw new RuntimeException(e);
        }
    }
    
    // BodyBuilder OverRide. Lots of Good stuff goes here
    // Creates Pmap for encoding
    @Override
    protected void bodyBuilder(MessageSchema schema, int cursor, int fragmentParaCount, CharSequence[] fragmentParaTypes, CharSequence[] fragmentParaArgs, CharSequence[] fragmentParaSuff){

        FieldReferenceOffsetManager from = MessageSchema.from(schema);
        
        //int[] initDictionary = from.newIntDefaultsDictionary();
        //int[] prevDictionary = from.newIntDefaultsDictionary();
                // replace dictionary with  number 
        
        int curCursor = cursor;
        int curCursor2 = cursor;
        boolean pmapOptional = false;

        for(int paramIdx = 0; paramIdx < fragmentParaCount - 1; paramIdx++) {
            int token = from.tokens[curCursor];
            int pmapType = TokenBuilder.extractType(token);
        
            String varName = new StringBuilder().append(fragmentParaArgs[paramIdx]).append(fragmentParaSuff[paramIdx]).toString();
            String varType = new StringBuilder().append(fragmentParaTypes[paramIdx]).toString();
            //int token = from.tokens[curCursor];
            
            //int pmapType = TokenBuilder.extractType(token);

            if(TypeMask.isInt(pmapType) == true) {
                try {
                    bodyTarget.append("activePmap = ");
                     } catch (IOException e) {
                        throw new RuntimeException(e);
                }
                encodePmapBuilderInt(schema, bodyTarget);
              
            }else if(TypeMask.isLong(pmapType) == true) {
                try {
                    bodyTarget.append("activePmap = ");
                     } catch (IOException e) {
                        throw new RuntimeException(e);
                }
                encodePmapBuilderLong(schema, bodyTarget);
  
            }else if(TypeMask.isText(pmapType) == true) {
                try {
                    bodyTarget.append("activePmap = ");
                     } catch (IOException e) {
                        throw new RuntimeException(e);
                }
                encodePmapBuilderString(schema, bodyTarget);
                
            }else if(TypeMask.isOptional(pmapType) == true) {
               pmapOptional = true;
               try {
                    bodyTarget.append("its optional");
                     } catch (IOException e) {
                        throw new RuntimeException(e);
                }
            }
            else{
                try {
                    bodyTarget.append("caught by nothing\n");
                     } catch (IOException e) {
                        throw new RuntimeException(e);
                }
            }
            curCursor +=  TypeMask.scriptTokenSize[TokenBuilder.extractType(token)];      
        }  
        
      /*  for(int paramIdx = 0; paramIdx<fragmentParaCount; paramIdx++) {
            int token = from.tokens[curCursor2];
            int pmapType = TokenBuilder.extractType(token);
            if(TypeMask.isInt(pmapType) == true) {
                int oper = TokenBuilder.extractOper(token);
                    switch (oper) {
                    case OperatorMask.Field_Copy:
                        copyIntGenerator(schema, bodyTarget);
        		break;
                    case OperatorMask.Field_Constant:
        		//this intentionally left blank, does nothing if constant
        		break;
                    case OperatorMask.Field_Default:
                        encodeDefaultIntGenerator(schema, bodyTarget);
        		break;
                    case OperatorMask.Field_Delta:
                        encodeDeltaIntGenerator(schema, bodyTarget);
        		break;
                    case OperatorMask.Field_Increment:
                        incrementIntGenerator(schema, bodyTarget);
        		break;
		}
            } else if(TypeMask.isLong(pmapType) == true) {
                int oper = TokenBuilder.extractOper(token);
                    switch (oper) {
                    case OperatorMask.Field_Copy:
                        copyLongGenerator(schema, bodyTarget);
        		break;
                    case OperatorMask.Field_Constant:
        		//this intentionally left blank, does nothing if constant
        		break;
                    case OperatorMask.Field_Default:
                        encodeDefaultLongGenerator(schema, bodyTarget);
        		break;
                    case OperatorMask.Field_Delta:
                        encodeDeltaLongGenerator(schema, bodyTarget);
        		break;
                    case OperatorMask.Field_Increment:
                        incrementLongGenerator(schema, bodyTarget);
        		break;
		}
            } //else if(TypeMask.isOptional(pmapType) == true) {
                else if(pmapOptional = true) {
                 
            }   
        }
*/
    }
}
            
