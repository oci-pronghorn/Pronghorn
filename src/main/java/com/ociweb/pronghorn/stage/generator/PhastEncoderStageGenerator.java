package com.ociweb.pronghorn.stage.generator;

import java.io.IOException;
import com.ociweb.pronghorn.stage.phast.PhastEncoder;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;

import static com.ociweb.pronghorn.util.Appendables.*;

import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.token.*;
import com.ociweb.pronghorn.pipe.util.build.TemplateProcessGeneratorLowLevelReader;


public class PhastEncoderStageGenerator extends TemplateProcessGeneratorLowLevelReader{

        private final Class encoder = PhastEncoder.class;    
        private final Appendable bodyTarget;        
        private final String longDictionaryName = "longDictionary";
        private final String intDictionaryName = "intDictiornary";
        private final String shortDictionaryName = "shortDictiornary";
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
    protected void encodePmapBuilderInt(MessageSchema schema, Appendable target) {
       //pmapBuilderInt(long pmap, int token, long curValue, long prevValue, long initValue, boolean isNull
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
    protected void encodePmapBuilderLong(MessageSchema schema, Appendable target) {
       //pmapBuilderLong(long pmap, int token, long curValue, long prevValue, long initValue, boolean isNull
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
    protected void encodePmapBuilderString(MessageSchema schema, Appendable target) {
       //pmapBuilderSringlong pmap, int token, long curValue, long prevValue, long initValue, boolean isNull
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
    protected void encodeIntPresentGenerator(MessageSchema schema, Appendable target) {
        //encodeIntPresent(DataOutputBlobWriter writer, long pmapHeader, int bitMask, int value)
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
    protected void encodeDeltaIntGenerator(MessageSchema schema, Appendable target) {
        //encodeDeltaInt(int[] intDictionary, DataOutputBlobWriter writer, long pmapHeader, int bitMask, int idx, int value)
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
    protected void encodeDeltaLongGenerator(MessageSchema schema, Appendable target) {
        //encodeDeltaLong(long[] longDictionary, DataOutputBlobWriter writer, long pmapHeader, int idx, int bitMask, long value)
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
    protected void encodeStringGenerator(MessageSchema schema, Appendable target) {
        //encodeString(DataOutputBlobWriter slab, DataOutputBlobWriter blob, String value)
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
    protected void incrementIntGenerator(MessageSchema schema, Appendable target) {
        //incrementInt(int[] intDictionary, DataOutputBlobWriter writer, long pmapHeader, int bitMask, int idx)
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
    protected void copyIntGenerator(MessageSchema schema, Appendable target) {
        //copyInt(int[] intDictionary, DataOutputBlobWriter writer, long pmapHeader, int bitMask, int idx)
        //copyInt(int[] intDictionary, DataOutputBlobWriter writer, long pmapHeader, int bitMask, int idx, int value){
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
    protected void encodeDefaultIntGenerator(MessageSchema schema, Appendable target) {
        //encodeDefaultInt(int[] defaultIntDictionary, DataOutputBlobWriter writer, long pmapHeader, int bitmask, int idx, int value)
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
    protected void encodeLongPresentGenerator(MessageSchema schema, Appendable target) {
        //encodeLongPresent(DataOutputBlobWriter writer, long pmapHeader, int bitMask, long value)
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
    protected void incrementLongGenerator(MessageSchema schema, Appendable target) {
        //incrementLong(long[] longDictionary, DataOutputBlobWriter writer, long pmapHeader, int bitMask, int idx)
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
    protected void copyLongGenerator(MessageSchema schema, Appendable target) {
        //copyLong(long[] longDictionary, DataOutputBlobWriter writer, long pmapHeader, int bitMask, int idx)
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
    protected void encodeDefaultLongGenerator(MessageSchema schema, Appendable target) {
        //encodeDefaultLong(long[] defaultLongDictionary, DataOutputBlobWriter writer, long pmapHeader, int bitmask, int idx, long value)
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
    protected void encodeShortPresentGenerator(MessageSchema schema, Appendable target) {
        //encodeShortPresent(DataOutputBlobWriter writer, long pmapHeader, int bitMask, short value)
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
    protected void incrementShortGenerator(MessageSchema schema, Appendable target) {
        //incrementShort(short[] shortDictionary, DataOutputBlobWriter writer, long pmapHeader, int bitMask, int idx)
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
    protected void copyShortGenerator(MessageSchema schema, Appendable target) {
        //copyShort(short[] shortDictionary, DataOutputBlobWriter writer, long pmapHeader, int bitMask, int idx)
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
    protected void encodeDefaultShortGenerator(MessageSchema schema, Appendable target) {
        //encodeDefaultShort(short[] defaultShortDictionary, DataOutputBlobWriter writer, long pmapHeader, int bitmask, int idx, short value)
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
    protected void encodeDeltaShortGenerator(MessageSchema schema, Appendable target) {
        //encodeDeltaShort(short[] shortDictionary, DataOutputBlobWriter writer, long pmapHeader, int idx, int bitMask, short value)
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
    
    @Override
    protected void bodyBuilder(MessageSchema schema, int cursor, int fragmentParaCount, CharSequence[] fragmentParaTypes, CharSequence[] fragmentParaArgs, CharSequence[] fragmentParaSuff)  {
    
        FieldReferenceOffsetManager from = MessageSchema.from(schema);
        
        int[] initDictionary = from.newIntDefaultsDictionary();
        int[] prevDictionary = from.newIntDefaultsDictionary();
                // replace dictionary with  number 
        
        int curCursor = cursor;
        int token = from.tokens[curCursor];
        int pmapType = TokenBuilder.extractType(token);
        boolean pmapOptional;
                
        long activePmap = 0;
        try {
            bodyTarget.append("long Pmap;");
         } catch (IOException e) {
            throw new RuntimeException(e);
        }
        for(int paramIdx = 0; paramIdx<fragmentParaCount; paramIdx++) {
            
            String varName = new StringBuilder().append(fragmentParaArgs[paramIdx]).append(fragmentParaSuff[paramIdx]).toString();
            String varType = new StringBuilder().append(fragmentParaTypes[paramIdx]).toString();
            //int token = from.tokens[curCursor];
            
            //int pmapType = TokenBuilder.extractType(token);

            if(TypeMask.isInt(pmapType) == true) {
                try {
                    bodyTarget.append("pmap = ");
                     } catch (IOException e) {
                        throw new RuntimeException(e);
                }
                encodePmapBuilderInt(schema, bodyTarget);
              
            }else if(TypeMask.isLong(pmapType) == true) {
                try {
                    bodyTarget.append("pmap = ");
                     } catch (IOException e) {
                        throw new RuntimeException(e);
                }
                encodePmapBuilderLong(schema, bodyTarget);
  
            }else if(TypeMask.isText(pmapType) == true) {
                try {
                    bodyTarget.append("pmap = ");
                     } catch (IOException e) {
                        throw new RuntimeException(e);
                }
                encodePmapBuilderString(schema, bodyTarget);
                
            }else if(TypeMask.isOptional(pmapType) == true) {
               pmapOptional = true;
            };

            //Good stuff goes here.
            //this comment is an example of what should be generated not executed here.
            //  activePmap = pmapBuilding( activePmap,  token.  <varName> , initDictionary[curCursor], prev??
             /// varName is the name of the variable that holds the cur value.
            //Which pmap should be called??? switch on varType.

            curCursor +=  TypeMask.scriptTokenSize[TokenBuilder.extractType(token)];
            
        }
        
        //now THE PMAP IS BUILT.
        for(int paramIdx = 0; paramIdx<fragmentParaCount; paramIdx++) {
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
            } else if(TypeMask.isOptional(pmapType) == true) {
                 
            }   
        }
        //NOW GENERATE WRITE IT CODE
    }
}
            
