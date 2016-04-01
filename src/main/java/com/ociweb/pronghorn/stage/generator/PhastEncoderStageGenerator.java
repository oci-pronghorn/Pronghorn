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
        private final String methodScope = "public";
        
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
    protected void encodeIntPresentGenerator(MessageSchema schema, Appendable target) {
        //encodeIntPresent(DataOutputBlobWriter writer, long pmapHeader, int bitMask, int value)
        try {
            appendStaticCall(target, encoder , "encodeIntPresent(")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName)                 
                    .append(intValueName).append(", ")
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
                    .append(bitMaskName)
                    .append(indexName).append(", ")                    
                    .append(intValueName).append(", ")
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
                    .append(bitMaskName)
                    .append(longValueName).append(", ")
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
                    .append(stringValueName).append(", ")
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
                    .append(bitMaskName)
                    .append(indexName).append(", ")
                    .append(");\n");
        } catch (IOException e) {
           throw new RuntimeException(e);
        }    
    }
    protected void copyIntGenerator(MessageSchema schema, Appendable target) {
        //copyInt(int[] intDictionary, DataOutputBlobWriter writer, long pmapHeader, int bitMask, int idx)
        try {
            appendStaticCall(target, encoder , "copyInt(")
                    .append(intDictionaryName).append(", ")
                    .append(writerName).append(", ")
                    .append(pmapName).append(", ")
                    .append(bitMaskName)
                    .append(indexName).append(", ")
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
                    .append(bitMaskName)
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
                    .append(bitMaskName)
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
                    .append(bitMaskName)
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
                    .append(bitMaskName)
                    .append(indexName).append(", ")
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
                    .append(bitMaskName)
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
                    .append(bitMaskName)
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
                    .append(bitMaskName)
                    .append(indexName).append(", ")
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
                    .append(bitMaskName)
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
                    .append(bitMaskName)                   
                    .append(shortValueName).append(", ")
                    .append(");\n");
        } catch (IOException e) {
           throw new RuntimeException(e);
        }
    }
}
            
