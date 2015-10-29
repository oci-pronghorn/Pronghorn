package com.ociweb.pronghorn.columns;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

public class BackingData<T> {

    private final long[]  longData;  //for serialization may want to store deltas as variable length values.
    private final int[]   intData;
    private final short[] shortData;
    private final byte[]  byteData;
    
    public final int longsPerRecord;
    public final int intsPerRecord;
    public final int shortsPerRecord;
    public final int bytesPerRecord;
    public final int recordCount;
    
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
        block.shortData[shortBase(recordIdx, block)+field.ordinal()] = value;
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
    
        
    private static int longBase(int recordIdx, BackingData block) {
        return recordIdx*block.longsPerRecord;
    }
    
    private static int intBase(int recordIdx, BackingData block) {
        return recordIdx*block.intsPerRecord;
    }
    
    private static int shortBase(int recordIdx, BackingData block) {
        return recordIdx*block.shortsPerRecord;
    }
    
    private static int byteBase(int recordIdx, BackingData block) {
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
            result |=   (1<<i.next().ordinal());
        }
        holder.byteData[absoluteOffset] = result;
    }
    

    public static <T extends Enum<T>, F extends Enum<F> & FieldsOf8Bits> boolean isEnumBitSetByte(F field, int recordIdx, BackingData<?> holder, T enumItem) {
        return 0 != (holder.byteData[byteBase(recordIdx, holder) + field.ordinal() ] & (1<<enumItem.ordinal()) );
    }


    public static void write(int recordIdx, int recordCount, BackingData<?> holder, DataOutput out) throws IOException {
        
        int base = longBase(recordIdx, holder);
        int count = holder.longsPerRecord*recordCount;        
        out.writeInt(count);
        while (--count>=0) {            
            out.writeLong(holder.longData[base++]);      
        }
        
        base = intBase(recordIdx, holder);
        count = holder.intsPerRecord*recordCount;        
        out.writeInt(count);
        while (--count>=0) {            
            out.writeInt(holder.intData[base++]);            
        }
        
        base = shortBase(recordIdx, holder);
        count = holder.shortsPerRecord*recordCount;
        out.writeInt(count);
        while (--count>=0) {            
            out.writeShort(holder.shortData[base++]);            
        }

        base = byteBase(recordIdx, holder);
        count = holder.bytesPerRecord*recordCount;
        out.writeInt(count);
        while (--count>=0) {            
            out.writeByte(holder.byteData[base++]);            
        }
        
    }

    public static void read(int recordIdx, BackingData<?> holder, DataInput in) throws IOException {
        
        int base = longBase(recordIdx, holder);
        int count = in.readInt();
        while (--count>=0) {            
            holder.longData[base++] = in.readLong();     
        }
        
        base = intBase(recordIdx, holder);
        count = in.readInt();
        while (--count>=0) {            
            holder.intData[base++] = in.readInt();           
        }
        
        base = shortBase(recordIdx, holder);
        count = in.readInt();
        while (--count>=0) {            
            holder.shortData[base++] = in.readShort();                 
        }

        base = byteBase(recordIdx, holder);
        count = in.readInt();
        while (--count>=0) {            
            holder.byteData[base++] = in.readByte();          
        }
    }


    
}
