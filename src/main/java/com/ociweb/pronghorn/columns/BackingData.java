package com.ociweb.pronghorn.columns;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import com.ociweb.pronghorn.pipe.util.hash.MurmurHash;

public  class  BackingData<T> {

    private final long[]  longData;  //for serialization may want to store deltas as variable length values.
    private final int[]   intData;
    private final short[] shortData;
    private final byte[]  byteData;
    
    public final int longsPerRecord;
    public final int intsPerRecord;
    public final int shortsPerRecord;
    public final int bytesPerRecord;
    
    public final int recordCount;
  
    private final static int HASH_SEED = 12345;
    
    //TODO - may want to add variable length vars
    //     - use non-fragmenting memory model
    //     - round up all memory to the next power of two before allocation.
    

    public <L extends Enum<L> & FieldsOf64Bits,
            I extends Enum<I> & FieldsOf32Bits,
            S extends Enum<S> & FieldsOf16Bits,
            B extends Enum<B> & FieldsOf8Bits> 
           BackingData( Class<L> longsEnum, Class<I> intsEnum, Class<S> shortsEnum, Class<B> bytesEnum, int recordCount) {
                
        
        this.longsPerRecord = null==longsEnum ? 0 : longsEnum.getEnumConstants().length;
        this.intsPerRecord = null==intsEnum ? 0 : intsEnum.getEnumConstants().length;
        this.shortsPerRecord = null==shortsEnum ? 0 : shortsEnum.getEnumConstants().length;
        this.bytesPerRecord = null==bytesEnum ? 0 : bytesEnum.getEnumConstants().length;
        
        this.longData  = new long[longsPerRecord*recordCount];
        this.intData   = new int[intsPerRecord*recordCount];
        this.shortData = new short[shortsPerRecord*recordCount];
        this.byteData  = new byte[bytesPerRecord*recordCount];
        
        this.recordCount = recordCount;
        
    } 
    
    public <L extends Enum<L> & FieldsOf64Bits,
            I extends Enum<I> & FieldsOf32Bits,
            S extends Enum<S> & FieldsOf16Bits,
            B extends Enum<B> & FieldsOf8Bits> BackingData(TypeDef<L,I,S,B> typeDef, int recordCount) {
        
        this.longsPerRecord = typeDef.longFieldCount;
        this.intsPerRecord = typeDef.intFieldCount;
        this.shortsPerRecord = typeDef.shortFieldCount;
        this.bytesPerRecord = typeDef.byteFieldCount;
        
        this.longData  = new long[longsPerRecord*recordCount];
        this.intData   = new int[intsPerRecord*recordCount];
        this.shortData = new short[shortsPerRecord*recordCount];
        this.byteData  = new byte[bytesPerRecord*recordCount];
        
        this.recordCount = recordCount;
        
    } 
    
    public long memoryConsumed() {        
        return (longData.length*8l) + (intData.length*4l) + (shortData.length*2l) + (byteData.length) + (5*4) + (8*4);
    }

    
    //for use by mutale flyweights when memory is a concern.
    public static <F extends Enum<F> & FieldsOf8Bits> void setByte(F field, byte value, int recordIdx, BackingData<?> block) {
        block.byteData[byteBase(recordIdx, block)+field.ordinal()] = value;
    }
    public static <F extends Enum<F> & FieldsOf8Bits> void incByte(F field, byte value, int recordIdx, BackingData<?> block) {
        block.byteData[byteBase(recordIdx, block)+field.ordinal()] += value;
    }
    public static <F extends Enum<F> & FieldsOf8Bits> void decByte(F field, byte value, int recordIdx, BackingData<?> block) {
        block.byteData[byteBase(recordIdx, block)+field.ordinal()] -= value;
    }

    public static <F extends Enum<F> & FieldsOf8Bits> byte getByte(F field, int recordIdx, BackingData<?> block) {
        return block.byteData[byteBase(recordIdx, block)+field.ordinal()];
    }
    
    public static <F extends Enum<F> & FieldsOf16Bits> void setShort(F field, short value, int recordIdx, BackingData<?> block) {
        int off = field.ordinal();
        setShort(off, value, recordIdx, block);
    }

    private static void setShort(int off, short value, int recordIdx, BackingData<?> block) {
        block.shortData[shortBase(recordIdx, block)+off] = value;
    }
    public static <F extends Enum<F> & FieldsOf16Bits> void incShort(F field, short value, int recordIdx, BackingData<?> block) {
        block.shortData[shortBase(recordIdx, block)+field.ordinal()] += value;
    }
    public static <F extends Enum<F> & FieldsOf16Bits> void decShort(F field, short value, int recordIdx, BackingData<?> block) {
        block.shortData[shortBase(recordIdx, block)+field.ordinal()] -= value;
    }
    public static <F extends Enum<F> & FieldsOf16Bits> short getShort(F field, int recordIdx, BackingData<?> block) {
        return block.shortData[shortBase(recordIdx, block)+field.ordinal()];
    }
    
    public static <F extends Enum<F> & FieldsOf16Bits> boolean isAllZeroShorts(int recordIdx, BackingData<?> block) {
        int offset = shortBase(recordIdx, block);
        int count = block.shortsPerRecord;
        short[] temp = block.shortData;
        int accum = 0;
        while (--count>=0) {
            accum |= temp[offset++];
        }
        return 0==accum;
    }    
    
    public static <F extends Enum<F> & FieldsOf32Bits> void setInt(F field, int value, int recordIdx, BackingData<?> block) {
        block.intData[intBase(recordIdx, block)+field.ordinal()] = value;
    }
    public static <F extends Enum<F> & FieldsOf32Bits> void incInt(F field, int value, int recordIdx, BackingData<?> block) {
        block.intData[intBase(recordIdx, block)+field.ordinal()] += value;
    }
    public static <F extends Enum<F> & FieldsOf32Bits> void decInt(F field, int value, int recordIdx, BackingData<?> block) {
        block.intData[intBase(recordIdx, block)+field.ordinal()] -= value;
    }
    public static <F extends Enum<F> & FieldsOf32Bits> int getInt(F field, int recordIdx, BackingData<?> block) {
        return block.intData[intBase(recordIdx, block)+field.ordinal()];
    }
    
    public static <F extends Enum<F> & FieldsOf32Bits> int getIntSum(F field, int[] recordIdxs, int base, BackingData<?> block) {
        
        int[] intDataLocal = block.intData;
        int off = field.ordinal();
        int intsPer = block.intsPerRecord;
        int sum = 0;
        int j = recordIdxs.length;
        while (--j>=0) {           
            sum += intDataLocal[((base + recordIdxs[j])*intsPer)+off];
        }
        return sum;
    }
    
    
    public static <F extends Enum<F> & FieldsOf64Bits> void setLong(F field, long value, int recordIdx, BackingData<?> block) {
        block.longData[longBase(recordIdx, block)+field.ordinal()] = value;
    }
    public static <F extends Enum<F> & FieldsOf64Bits> void incLong(F field, long value, int recordIdx, BackingData<?> block) {
        block.longData[longBase(recordIdx, block)+field.ordinal()] += value;
    }
    public static <F extends Enum<F> & FieldsOf64Bits> void decLong(F field, long value, int recordIdx, BackingData<?> block) {
        block.longData[longBase(recordIdx, block)+field.ordinal()] -= value;
    }

    public static <F extends Enum<F> & FieldsOf64Bits> long getLong(F field, int recordIdx, BackingData<?> block) {
        return block.longData[longBase(recordIdx, block)+field.ordinal()];
    }
    
        
    private static int longBase(int recordIdx, BackingData<?> block) {
        return recordIdx*block.longsPerRecord;
    }    
    private static int intBase(int recordIdx, BackingData<?> block) {
        return recordIdx*block.intsPerRecord;
    }    
    private static int shortBase(int recordIdx, BackingData<?> block) {
        return recordIdx*block.shortsPerRecord;
    }    
    private static int byteBase(int recordIdx, BackingData<?> block) {
        return recordIdx*block.bytesPerRecord;
    }

    public static <T extends Enum<T>, F extends Enum<F> & FieldsOf8Bits> T getEnumBytes(F field, int recordIdx, BackingData<?> holder,  Class<T> clazz) {        
        return getEnumBytes(holder, clazz, byteBase(recordIdx, holder) + field.ordinal());
    }
    private static <T extends Enum<T>> T getEnumBytes(BackingData<?> holder, Class<T> clazz, int absoluteOffset) {
        return (T)clazz.getEnumConstants()[holder.byteData[ absoluteOffset ]];
    }


    public static  <T extends Enum<T>, F extends Enum<F> & FieldsOf8Bits> void setEnumBytes(F field,  int recordIdx, BackingData<?> holder, T value) {        
        setEnumBytes(holder, value, byteBase(recordIdx, holder) + field.ordinal());
    }
    private static <T extends Enum<T>> void setEnumBytes(BackingData<?> holder, T value, int absoluteOffset) {
        holder.byteData[ absoluteOffset ] = (byte)value.ordinal();
    }


    public static <T extends Enum<T>, F extends Enum<F> & FieldsOf8Bits> void setEnumSetBytes(F field, int recordIdx, BackingData<?> holder, T enum1) {        
        setEnumSetBytes(holder, byteBase(recordIdx, holder) + field.ordinal(), enum1);
    }

    private static <T extends Enum<T>> void setEnumSetBytes(BackingData<?> holder, int absoluteOffset, T enum1) {
        holder.byteData[absoluteOffset] = (byte)(1<<enum1.ordinal());
    }
    
    public static <T extends Enum<T>, F extends Enum<F> & FieldsOf8Bits> void setEnumSetBytes(F field, int recordIdx, BackingData<?> holder, T enum1, T enum2) {        
        setEnumSetBytes(holder, byteBase(recordIdx, holder) + field.ordinal(), enum1, enum2);
    }

    private static <T extends Enum<T>> void setEnumSetBytes(BackingData<?> holder, int absoluteOffset, T enum1, T enum2) {
        byte result = 0;
        result |=   (1<<enum1.ordinal());
        result |=   (1<<enum2.ordinal());
        
        holder.byteData[absoluteOffset] = result;
    }
    
    public static <T extends Enum<T>, F extends Enum<F> & FieldsOf8Bits> void setEnumSetBytes(F field, int recordIdx, BackingData<?> holder, T enum1, T enum2, T enum3) {        
        setEnumSetBytes(holder, byteBase(recordIdx, holder) + field.ordinal(), enum1, enum2, enum3);
    }

    private static <T extends Enum<T>> void setEnumSetBytes(BackingData<?> holder, int absoluteOffset, T enum1, T enum2, T enum3) {
        byte result = 0;
        result |=   (1<<enum1.ordinal());
        result |=   (1<<enum2.ordinal());
        result |=   (1<<enum3.ordinal());
        
        holder.byteData[absoluteOffset] = result;
    }
    
    public static <T extends Enum<T>, F extends Enum<F> & FieldsOf8Bits> void setEnumSetBytes(F field, int recordIdx, BackingData<?> holder, T enum1, T enum2, T enum3, T enum4) {        
        setEnumSetBytes(holder, byteBase(recordIdx, holder) + field.ordinal(), enum1, enum2, enum3, enum4);
    }

    private static <T extends Enum<T>> void setEnumSetBytes(BackingData<?> holder, int absoluteOffset, T enum1, T enum2, T enum3, T enum4) {
        byte result = 0;
        result |=   (1<<enum1.ordinal());
        result |=   (1<<enum2.ordinal());
        result |=   (1<<enum3.ordinal());
        result |=   (1<<enum4.ordinal());
                
        holder.byteData[absoluteOffset] = result;
    }   
    
    public static <T extends Enum<T>, F extends Enum<F> & FieldsOf8Bits> void setEnumSetBytes(F field, int recordIdx, BackingData<?> holder, T enum1, T enum2, T enum3, T enum4, T enum5) {        
        setEnumSetBytes(holder, byteBase(recordIdx, holder) + field.ordinal(), enum1, enum2, enum3, enum4, enum5);
    }

    private static <T extends Enum<T>> void setEnumSetBytes(BackingData<?> holder, int absoluteOffset, T enum1, T enum2, T enum3, T enum4, T enum5) {
        byte result = 0;
        result |=   (1<<enum1.ordinal());
        result |=   (1<<enum2.ordinal());
        result |=   (1<<enum3.ordinal());
        result |=   (1<<enum4.ordinal());
        result |=   (1<<enum5.ordinal());
                        
        holder.byteData[absoluteOffset] = result;
    }   
    
    public static <T extends Enum<T>, F extends Enum<F> & FieldsOf8Bits> void setEnumSetBytes(F field, int recordIdx, BackingData<?> holder, Set<T> enumSet) {
        setEnumSetBytes(holder, byteBase(recordIdx, holder) + field.ordinal(), enumSet);
    }

    private static <T extends Enum<T>> void setEnumSetBytes(BackingData<?> holder, int absoluteOffset, Set<T> enumSet) {
        byte result = 0;
        Iterator<T> i = enumSet.iterator();
        while (i.hasNext()) {
            result |= (1<<i.next().ordinal());
        }
        holder.byteData[absoluteOffset] = result;
    }    

    public static <T extends Enum<T>, F extends Enum<F> & FieldsOf8Bits> boolean isEnumBitSetByte(F field, int recordIdx, BackingData<?> holder, T enumItem) {
        return 0 != (holder.byteData[byteBase(recordIdx, holder) + field.ordinal() ] & (1<<enumItem.ordinal()) );
    }



    public final void write(int recordIdx, int recordCount, DataOutput out) throws IOException {
        if (longsPerRecord>0){        
            writeLongs(out, longBase(recordIdx, this), longsPerRecord*recordCount, longData);
        }
        if (intsPerRecord>0) {
            writeInts(out, intBase(recordIdx, this), intsPerRecord*recordCount, intData);
        }
        if (shortsPerRecord>0) {
            writeShorts(out, shortBase(recordIdx, this), shortsPerRecord*recordCount, shortData);
        }
        if (bytesPerRecord>0) {
            writeBytes(out, byteBase(recordIdx, this), bytesPerRecord*recordCount, byteData);
        }
    }
    
    public final void read(int recordIdx, int exepectedRecordCount, DataInput in) throws IOException {
        if (longsPerRecord>0) { 
            readLongs(in, longBase(recordIdx, this), longsPerRecord*exepectedRecordCount, longData);
        }
        if (intsPerRecord>0) {
            readInts(in, intBase(recordIdx, this), intsPerRecord*exepectedRecordCount, intData);
        }
        if (shortsPerRecord>0) {
            readShorts(in, shortBase(recordIdx, this), shortsPerRecord*exepectedRecordCount, shortData);
        }
        if (bytesPerRecord>0) {
            readBytes(in, byteBase(recordIdx, this), bytesPerRecord*exepectedRecordCount, byteData);
        }
    }

    protected void writeBytes(DataOutput out, int base, int count, byte[] byteData) throws IOException {
        out.writeInt(count);
        out.write(byteData, base, count);
    }

    protected void writeShorts(DataOutput out, int base, int count, short[] shortData) throws IOException {
        out.writeInt(count);
        while (--count>=0) {            
            out.writeShort(shortData[base++]);            
        }
    }

    protected void writeInts(DataOutput out, int base, int count, int[] intData) throws IOException {
        out.writeInt(count);
        while (--count>=0) {            
            out.writeInt(intData[base++]);            
        }
    }

    protected void writeLongs(DataOutput out, int base, int count, long[] longData) throws IOException {
        out.writeInt(count);
        while (--count>=0) {            
            out.writeLong(longData[base++]);      
        }
    }

    protected void readBytes(DataInput in, int base, int expectedByteCount, byte[] byteData) throws IOException {
        int count = in.readInt();
        assert(count == expectedByteCount) : "expected different count of records";
        in.readFully(byteData, base, expectedByteCount);
    }

    protected void readShorts(DataInput in, int base, int expectedShortCount, short[] shortData) throws IOException {
        int count = in.readInt();
        assert(count == expectedShortCount) : "expected different count of records";
        while (--count>=0) {            
            shortData[base++] = in.readShort();                 
        }
    }

    protected void readInts(DataInput in, int base, int expectedIntCount, int[] intData) throws IOException {
        int count = in.readInt();
        assert(count == expectedIntCount) : "expected different count of records";
        while (--count>=0) {            
            intData[base++] = in.readInt();           
        }
    }

    protected void readLongs(DataInput in, int base, int expectedLongCount, long[] longData) throws IOException {
        int count = in.readInt();
        assert(count == expectedLongCount) : "expected different count of records";
        while (--count>=0) {            
            longData[base++] = in.readLong();     
        }
    }

    
    public static int hash(int recordIdx, BackingData<?> backing) {
    
        int result = 0;
        
        result +=  MurmurHash.hash32(backing.byteData, byteBase(recordIdx, backing), backing.bytesPerRecord, HASH_SEED);
        result +=  MurmurHash.hash32(backing.shortData, shortBase(recordIdx, backing), backing.shortsPerRecord, HASH_SEED);
        result +=  MurmurHash.hash32(backing.intData, intBase(recordIdx, backing), backing.intsPerRecord, HASH_SEED);
        result +=  MurmurHash.hash32(backing.longData, longBase(recordIdx, backing), backing.longsPerRecord, HASH_SEED);
        
        return result;
    }
    
    public static int hash(int recordIdx, int recordCount, BackingData<?> backing) {
        int result = 0;
        
        int i = recordCount;
        while (--i>=0) {
            result += hash(recordIdx++,backing);
        }
        return result;
    }
    
    public static boolean equals(int recordIdxA, int recordIdxB, int count, BackingData<?> backing) {
        if (recordIdxA != recordIdxB) {
            int i; 
            int baseA;
            int baseB;
            
            i = (backing.bytesPerRecord*count);
            baseA = byteBase(recordIdxA,backing);
            baseB = byteBase(recordIdxB,backing);
            while (--i>=0) {
                if (backing.byteData[baseA+i]!=backing.byteData[baseB+i]) {
                    return false;
                }
            }
            
            i = (backing.shortsPerRecord*count);
            baseA = shortBase(recordIdxA,backing);
            baseB = shortBase(recordIdxB,backing);
            while (--i>=0) {
                if (backing.shortData[baseA+i]!=backing.shortData[baseB+i]) {
                    return false;
                }
            }
            
            i = (backing.intsPerRecord*count);
            baseA = intBase(recordIdxA,backing);
            baseB = intBase(recordIdxB,backing);
            while (--i>=0) {
                if (backing.intData[baseA+i]!=backing.intData[baseB+i]) {
                    return false;
                }
            }
            
            i = (backing.longsPerRecord*count);
            baseA = longBase(recordIdxA,backing);
            baseB = longBase(recordIdxB,backing);
            while (--i>=0) {
                if (backing.longData[baseA+i]!=backing.longData[baseB+i]) {
                    return false;
                }
            }
        }
        return true;
    }
    
    //TODO: Perhaps we should hold these so this is neverneeded again.
    
    public static  <L extends Enum<L> & FieldsOf64Bits,
                    I extends Enum<I> & FieldsOf32Bits,
                    S extends Enum<S> & FieldsOf16Bits,
                    B extends Enum<B> & FieldsOf8Bits> 
                                    String toString(TypeDef<L,I,S,B> typeDef, int recordIdx, BackingData<?> backing) {
        StringBuilder builder = new StringBuilder();
        for(L item: typeDef.longFields) {
            builder.append(item).append('=').append(backing.longData[longBase(recordIdx, backing)+item.ordinal()]).append("\n");            
        }
        for(I item: typeDef.intFields) {
            builder.append(item).append('=').append(backing.intData[intBase(recordIdx, backing)+item.ordinal()]).append("\n");            
        }
        for(S item: typeDef.shortFields) {
            builder.append(item).append('=').append(backing.shortData[shortBase(recordIdx, backing)+item.ordinal()]).append("\n");            
        }
        for(B item: typeDef.byteFields) {
            builder.append(item).append('=').append(backing.byteData[shortBase(recordIdx, backing)+item.ordinal()]).append("\n");            
        }
        return builder.toString();
    }


    
}
