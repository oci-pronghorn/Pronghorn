package com.ociweb.pronghorn.stage.generator;

import java.io.IOException;

import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.util.build.TemplateProcessGeneratorLowLevelReader;

public class PhastEncoderStageGenerator extends TemplateProcessGeneratorLowLevelReader{

    public PhastEncoderStageGenerator(MessageSchema schema, Appendable bodyTarget) {
        super(schema, bodyTarget);
        additionalImports(schema, bodyTarget);
        encodeIntPresentGenerator(schema, bodyTarget);
        encodeLongPresentGenerator(schema, bodyTarget);
        encodeShortPresentGenerator(schema, bodyTarget);
        encodeDeltaIntGenerator(schema, bodyTarget);
        encodeDeltaLongGenerator(schema, bodyTarget);
        encodeDeltaShortGenerator(schema, bodyTarget);
        copyIntGenerator(schema, bodyTarget);
        copyLongGenerator(schema, bodyTarget);
        copyShortGenerator(schema, bodyTarget);
        encodeDefaultIntGenerator(schema, bodyTarget);
        encodeDefaultLongGenerator(schema, bodyTarget);
        encodeDefaultShortGenerator(schema, bodyTarget);
        encodeStringGenerator(schema, bodyTarget);
        incrementIntGenerator(schema, bodyTarget);
        incrementLongGenerator(schema, bodyTarget);
        incrementShortGenerator(schema, bodyTarget);
        
    }

    @Override
    protected void additionalImports(MessageSchema schema, Appendable target) {
        try {
            target.append("import ").append(schema.getClass().getCanonicalName()).append(";\n");
        } catch (IOException e) {
           throw new RuntimeException(e);
        }
    }
    protected void encodeIntPresentGenerator(MessageSchema schema, Appendable target) {
        //encodeIntPresent(DataOutputBlobWriter writer, long pmapHeader, int bitMask, int value)
        try {
            target.append("encodeIntPresent(").append("DataOutputBlobWriter ").append(schema.getClass().getCanonicalName())
                    .append(", long ").append(schema.getClass().getCanonicalName())
                    .append(", int ").append(schema.getClass().getCanonicalName())
                    .append(", int ").append(schema.getClass().getCanonicalName())
                    .append(";\n");
        } catch (IOException e) {
           throw new RuntimeException(e);
        }
    }
    protected void encodeDeltaIntGenerator(MessageSchema schema, Appendable target) {
        //encodeDeltaInt(int[] intDictionary, DataOutputBlobWriter writer, long pmapHeader, int bitMask, int idx, int value)
        try {
            target.append("encodeDeltaInt(int[] ").append(schema.getClass().getCanonicalName())
                    .append("DataOutputBlobWriter ").append(schema.getClass().getCanonicalName())
                    .append(", long ").append(schema.getClass().getCanonicalName())
                    .append(", int ").append(schema.getClass().getCanonicalName())
                    .append(", int ").append(schema.getClass().getCanonicalName())
                    .append(", int ").append(schema.getClass().getCanonicalName())
                    .append(";\n");
        } catch (IOException e) {
           throw new RuntimeException(e);
        }
    }
    protected void encodeDeltaLongGenerator(MessageSchema schema, Appendable target) {
        //encodeDeltaLong(long[] longDictionary, DataOutputBlobWriter writer, long pmapHeader, int idx, int bitMask, long value)
        try {
            target.append("encodeDeltaLong(long[] ").append(schema.getClass().getCanonicalName())
                    .append("DataOutputBlobWriter ").append(schema.getClass().getCanonicalName())
                    .append(", long ").append(schema.getClass().getCanonicalName())
                    .append(", int ").append(schema.getClass().getCanonicalName())
                    .append(", int ").append(schema.getClass().getCanonicalName())
                    .append(", long ").append(schema.getClass().getCanonicalName())
                    .append(";\n");
        } catch (IOException e) {
           throw new RuntimeException(e);
        }
    }
    protected void encodeStringGenerator(MessageSchema schema, Appendable target) {
        //encodeString(DataOutputBlobWriter slab, DataOutputBlobWriter blob, String value)
        try {
            target.append("encodeString(").append("DataOutputBlobWriter ").append(schema.getClass().getCanonicalName())
                    .append("DataOutputBlobWriter ").append(schema.getClass().getCanonicalName())
                    .append(", string ").append(schema.getClass().getCanonicalName())
                    .append(";\n");
        } catch (IOException e) {
           throw new RuntimeException(e);
        }
    }
    protected void incrementIntGenerator(MessageSchema schema, Appendable target) {
            //incrementInt(int[] intDictionary, DataOutputBlobWriter writer, long pmapHeader, int bitMask, int idx)
        try {
            target.append("incrementInt(int[] ").append(schema.getClass().getCanonicalName())
                    .append("DataOutputBlobWriter ").append(schema.getClass().getCanonicalName())
                    .append(", long ").append(schema.getClass().getCanonicalName())
                    .append(", int ").append(schema.getClass().getCanonicalName())
                    .append(", int ").append(schema.getClass().getCanonicalName())
                    .append(";\n");
        } catch (IOException e) {
           throw new RuntimeException(e);
        }    
    }
    protected void copyIntGenerator(MessageSchema schema, Appendable target) {
        //copyInt(int[] intDictionary, DataOutputBlobWriter writer, long pmapHeader, int bitMask, int idx)
        try {
            target.append("copyInt(int[] ").append(schema.getClass().getCanonicalName())
                    .append("DataOutputBlobWriter ").append(schema.getClass().getCanonicalName())
                    .append(", long ").append(schema.getClass().getCanonicalName())
                    .append(", int ").append(schema.getClass().getCanonicalName())
                    .append(", int ").append(schema.getClass().getCanonicalName())
                    .append(";\n");
        } catch (IOException e) {
           throw new RuntimeException(e);
        }    
    }
    protected void encodeDefaultIntGenerator(MessageSchema schema, Appendable target) {
        //encodeDefaultInt(int[] defaultIntDictionary, DataOutputBlobWriter writer, long pmapHeader, int bitmask, int idx, int value)
        try {
            target.append("encodeDefaultInt(int[] ").append(schema.getClass().getCanonicalName())
                    .append("DataOutputBlobWriter ").append(schema.getClass().getCanonicalName())
                    .append(", long ").append(schema.getClass().getCanonicalName())
                    .append(", int ").append(schema.getClass().getCanonicalName())
                    .append(", int ").append(schema.getClass().getCanonicalName())
                    .append(", int ").append(schema.getClass().getCanonicalName())
                    .append(";\n");
        } catch (IOException e) {
           throw new RuntimeException(e);
        }    
    }
    protected void encodeLongPresentGenerator(MessageSchema schema, Appendable target) {
        //encodeLongPresent(DataOutputBlobWriter writer, long pmapHeader, int bitMask, long value)
        try {
            target.append("encodeLongPresent(").append("DataOutputBlobWriter ").append(schema.getClass().getCanonicalName())
                    .append(", long ").append(schema.getClass().getCanonicalName())
                    .append(", int ").append(schema.getClass().getCanonicalName())
                    .append(", long ").append(schema.getClass().getCanonicalName())
                    .append(";\n");
        } catch (IOException e) {
           throw new RuntimeException(e);
        }
    }
    protected void incrementLongGenerator(MessageSchema schema, Appendable target) {
        //incrementLong(long[] longDictionary, DataOutputBlobWriter writer, long pmapHeader, int bitMask, int idx)
        try {
            target.append("incrementLong(long[] ").append(schema.getClass().getCanonicalName())
                    .append("DataOutputBlobWriter ").append(schema.getClass().getCanonicalName())
                    .append(", long ").append(schema.getClass().getCanonicalName())
                    .append(", int ").append(schema.getClass().getCanonicalName())
                    .append(", int ").append(schema.getClass().getCanonicalName())
                    .append(";\n");
        } catch (IOException e) {
           throw new RuntimeException(e);
        }    
    }
    protected void copyLongGenerator(MessageSchema schema, Appendable target) {
        //copyLong(long[] longDictionary, DataOutputBlobWriter writer, long pmapHeader, int bitMask, int idx)
        try {
            target.append("copyLong(long[] ").append(schema.getClass().getCanonicalName())
                    .append("DataOutputBlobWriter ").append(schema.getClass().getCanonicalName())
                    .append(", long ").append(schema.getClass().getCanonicalName())
                    .append(", int ").append(schema.getClass().getCanonicalName())
                    .append(", int ").append(schema.getClass().getCanonicalName())
                    .append(";\n");
        } catch (IOException e) {
           throw new RuntimeException(e);
        }   
    }
    protected void encodeDefaultLongGenerator(MessageSchema schema, Appendable target) {
        //encodeDefaultLong(long[] defaultLongDictionary, DataOutputBlobWriter writer, long pmapHeader, int bitmask, int idx, long value)
        try {
            target.append("encodeDefaultLong(long[] ").append(schema.getClass().getCanonicalName())
                    .append("DataOutputBlobWriter ").append(schema.getClass().getCanonicalName())
                    .append(", long ").append(schema.getClass().getCanonicalName())
                    .append(", int ").append(schema.getClass().getCanonicalName())
                    .append(", int ").append(schema.getClass().getCanonicalName())
                    .append(", long ").append(schema.getClass().getCanonicalName())
                    .append(";\n");
        } catch (IOException e) {
           throw new RuntimeException(e);
        }   
    }
    protected void encodeShortPresentGenerator(MessageSchema schema, Appendable target) {
        //encodeShortPresent(DataOutputBlobWriter writer, long pmapHeader, int bitMask, short value)
        try {
            target.append("encodeShortPresent(").append("DataOutputBlobWriter ").append(schema.getClass().getCanonicalName())
                    .append(", long ").append(schema.getClass().getCanonicalName())
                    .append(", int ").append(schema.getClass().getCanonicalName())
                    .append(", short ").append(schema.getClass().getCanonicalName())
                    .append(";\n");
        } catch (IOException e) {
           throw new RuntimeException(e);
        }
    }
    protected void incrementShortGenerator(MessageSchema schema, Appendable target) {
        //incrementShort(short[] shortDictionary, DataOutputBlobWriter writer, long pmapHeader, int bitMask, int idx)
        try {
            target.append("incrementShort(short[] ").append(schema.getClass().getCanonicalName())
                    .append("DataOutputBlobWriter ").append(schema.getClass().getCanonicalName())
                    .append(", long ").append(schema.getClass().getCanonicalName())
                    .append(", int ").append(schema.getClass().getCanonicalName())
                    .append(", int ").append(schema.getClass().getCanonicalName())
                    .append(";\n");
        } catch (IOException e) {
           throw new RuntimeException(e);
        } 
    }
    protected void copyShortGenerator(MessageSchema schema, Appendable target) {
        //copyShort(short[] shortDictionary, DataOutputBlobWriter writer, long pmapHeader, int bitMask, int idx)
        try {
            target.append("copyShort(short[] ").append(schema.getClass().getCanonicalName())
                    .append("DataOutputBlobWriter ").append(schema.getClass().getCanonicalName())
                    .append(", long ").append(schema.getClass().getCanonicalName())
                    .append(", int ").append(schema.getClass().getCanonicalName())
                    .append(", int ").append(schema.getClass().getCanonicalName())
                    .append(";\n");
        } catch (IOException e) {
           throw new RuntimeException(e);
        }   
    }
    protected void encodeDefaultShortGenerator(MessageSchema schema, Appendable target) {
        //encodeDefaultShort(short[] defaultShortDictionary, DataOutputBlobWriter writer, long pmapHeader, int bitmask, int idx, short value)
        try {
            target.append("encodeDefaultShort(short[] ").append(schema.getClass().getCanonicalName())
                    .append("DataOutputBlobWriter ").append(schema.getClass().getCanonicalName())
                    .append(", long ").append(schema.getClass().getCanonicalName())
                    .append(", int ").append(schema.getClass().getCanonicalName())
                    .append(", int ").append(schema.getClass().getCanonicalName())
                    .append(", short ").append(schema.getClass().getCanonicalName())
                    .append(";\n");
        } catch (IOException e) {
           throw new RuntimeException(e);
        }   
    }
    protected void encodeDeltaShortGenerator(MessageSchema schema, Appendable target) {
        //encodeDeltaShort(short[] shortDictionary, DataOutputBlobWriter writer, long pmapHeader, int idx, int bitMask, short value)
         try {
            target.append("encodeDeltaShort(short[] ").append(schema.getClass().getCanonicalName())
                    .append("DataOutputBlobWriter ").append(schema.getClass().getCanonicalName())
                    .append(", long ").append(schema.getClass().getCanonicalName())
                    .append(", int ").append(schema.getClass().getCanonicalName())
                    .append(", int ").append(schema.getClass().getCanonicalName())
                    .append(", short ").append(schema.getClass().getCanonicalName())
                    .append(";\n");
        } catch (IOException e) {
           throw new RuntimeException(e);
        }
    }
}
            
