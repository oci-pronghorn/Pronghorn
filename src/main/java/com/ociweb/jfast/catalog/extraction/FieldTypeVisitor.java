package com.ociweb.jfast.catalog.extraction;

import java.nio.MappedByteBuffer;

import com.ociweb.jfast.field.TypeMask;

public class FieldTypeVisitor implements ExtractionVisitor{

    //High                       1
    static final int BITS_DECIMAL = 11;
    static final int BITS_SIGN    =    3;
    static final int BITS_DOT     =      3;
    static final int BITS_COMMA   =        3;
    static final int BITS_ASCII   =          3;
    static final int BITS_OTHER   =            3;   
    
    static final int SHIFT_OTHER   = 0;
    static final int SHIFT_ASCII   = BITS_OTHER+SHIFT_OTHER;
    static final int SHIFT_COMMA   = BITS_ASCII+SHIFT_ASCII;
    static final int SHIFT_DOT     = BITS_COMMA+SHIFT_COMMA;
    static final int SHIFT_SIGN    = BITS_DOT+SHIFT_DOT;
    static final int SHIFT_DECIMAL = BITS_SIGN+SHIFT_SIGN;
        
    static final int ONE_DECIMAL = 1<<SHIFT_DECIMAL; //0010 0000 0000 0000
    static final int ONE_SIGN    = 1<<SHIFT_SIGN;    //0000 0100 0000 0000
    static final int ONE_DOT     = 1<<SHIFT_DOT;     //0000 0000 1000 0000
    static final int ONE_COMMA   = 1<<SHIFT_COMMA;   //0000 0000 0001 0000
    static final int ONE_ASCII   = 1<<SHIFT_ASCII;   //0000 0000 0000 0100
    static final int ONE_OTHER   = 1<<SHIFT_OTHER;   //0000 0000 0000 0001
               
    static final int   ACCUM_MASK = (((1<<(BITS_DECIMAL-1))-1)<<SHIFT_DECIMAL) |
                                    (((1<<(BITS_SIGN-1))-1)<<SHIFT_SIGN) |
                                    (((1<<(BITS_DOT-1))-1)<<SHIFT_DOT) |
                                    (((1<<(BITS_COMMA-1))-1)<<SHIFT_COMMA) |
                                    (((1<<(BITS_ASCII-1))-1)<<SHIFT_ASCII) |
                                    (((1<<(BITS_OTHER-1))-1)<<SHIFT_OTHER);
    
    private static final int TYPE_UINT = TypeMask.IntegerUnsigned>>1; //  0
    private static final int TYPE_SINT = TypeMask.IntegerSigned>>1;   //  1
    private static final int TYPE_ULONG = TypeMask.LongUnsigned>>1;   //  2
    private static final int TYPE_SLONG = TypeMask.LongSigned>>1;     //  3
    private static final int TYPE_ASCII = TypeMask.TextASCII>>1;      //  4 
    private static final int TYPE_BYTES = TypeMask.TextUTF8>>1;       //  5 
    private static final int TYPE_DECIMAL = TypeMask.Decimal>>1;      //  6
    private static final int TYPE_NULL = 7;//no need to use BYTE_ARRAY, its the same as UTF8
    //NOTE: more will be added here for group and sequence once JSON support is added
    private static final int TYPE_EOM = 15;
    
    
    //TODO: saturation isnot working and rolls over to zero then gets masked.
    
    int         activeSum;
    int         activeLength;
    boolean     activeQuote;
    StringBuilder temp = new StringBuilder();
    
    final int[] accumValues;
    
    //16 possible field types
    final int typeTrieUnit = 16;
    final int[] typeTrie;
    int typeTrieCursor; 
    int typeTrieLimit = typeTrieUnit;
    
    
    public FieldTypeVisitor() {
        System.err.println(Integer.toBinaryString(ACCUM_MASK));
        
        //Pull out with its own class testing and tostring
        typeTrie = new int[1<<20];//1M room for all the fields
        
        //one value for each of the possible bytes we may encounter.
        accumValues = new int[256];
        int i = 256;
        while (--i>=0) {
            accumValues[i] = ONE_OTHER;
        }
        i = 127;
        while (--i>=0) {
            accumValues[i] = ONE_ASCII;
        }
        i = 58;
        while (--i>=48) {
            accumValues[i] = ONE_DECIMAL;
        }
        accumValues[(int)'+'] = ONE_SIGN;
        accumValues[(int)'-'] = ONE_SIGN;        
        accumValues[(int)'.'] = ONE_DOT;
        accumValues[(int)','] = ONE_COMMA; //required when comma is not the delimiter to support thousands marking in US english
                
        resetFieldSum();
        
        typeTrieCursor=0;
    }

    private void resetFieldSum() {
        activeLength = 0;
        activeSum = 0;
        activeQuote = false;
        temp.setLength(0);
    }
    
    @Override
    public void appendContent(MappedByteBuffer mappedBuffer, int pos, int limit, boolean contentQuoted) {
        
        activeQuote |= contentQuoted;
        
        int i = (limit-pos);
        activeLength+=i;
        while (--i>=0) {
            byte b = mappedBuffer.get(pos+i);
            temp.append((char)b);
            activeSum = (activeSum + accumValues[0xFF&b]) & ACCUM_MASK;            
        };        
        
    }

    @Override
    public void closeRecord() {
        
        typeTrie[typeTrieCursor+TYPE_EOM]++;        
        typeTrieCursor=0;
    }

    @Override
    public void closeField() {
        assert(activeLength>=0);
        
        System.err.println(Integer.toBinaryString(activeSum));
        
        //split fields.
        int otherCount = ((1<<BITS_OTHER)-1)&(activeSum>>SHIFT_OTHER);
        int decimalCount = ((1<<BITS_DECIMAL)-1)&(activeSum>>SHIFT_DECIMAL);
        int asciiCount = ((1<<BITS_ASCII)-1)&(activeSum>>SHIFT_ASCII);
        int signCount = ((1<<BITS_SIGN)-1)&(activeSum>>SHIFT_SIGN);
        
        //NOTE: swap these two assignments for British vs American numbers
        int dotCount = ((1<<BITS_DOT)-1)&(activeSum>>SHIFT_DOT);
        int commaCount = ((1<<BITS_COMMA)-1)&(activeSum>>SHIFT_COMMA);
                     
        
  //      System.err.println(decimalCount+" "+signCount+" "+dotCount+" "+commaCount+" "+asciiCount+" "+otherCount);
        
        //Need flag to turn on that feature
        ///TODO: convert short byte sequences to int or long
        ///TODO: treat leading zero as ascii not numeric.
        
        //apply rules to determine field type
        int type;
        if (activeLength==0 || activeSum==0) {
            //null field
            type = TYPE_NULL; 
            System.err.println("null");
        } else {        
            if (otherCount>0) {
                //utf8 or byte array
                type = TYPE_BYTES;
            } else { 
                if (asciiCount>0 || activeQuote || signCount>1 || dotCount>1 || decimalCount>18) { //NOTE: 18 could be optimized by reading first digit
                    //ascii text
                    type = TYPE_ASCII;
                } else {
                    if (dotCount==1) {
                        //decimal 
                        type = TYPE_DECIMAL;
                    } else {  
                        //no dot
                        if (decimalCount>9) { //NOTE: 9 could be optimized by reading first digit
                            //long
                            if (signCount==0) {
                                //unsigned
                                type = TYPE_ULONG;
                            } else {
                                //signed
                                type = TYPE_SLONG;
                            }
                        } else {
                            //int
                            if (signCount==0) {
                                //unsigned
                                type = TYPE_UINT;
                            } else {
                                //signed
                                type = TYPE_SINT;
                            }                            
                        }
                    }                
                }           
            }
        }
        String v = (type==TYPE_NULL ? "NULL" : TypeMask.methodTypeName[type<<1]);
        System.err.println(v+" from "+temp.toString());
        
        //it points to next which may end up holding the count.
        
        int pos = typeTrieCursor+type;
        if (typeTrie[pos]==0) {
            //create new position
            typeTrieCursor = typeTrie[pos] = typeTrieLimit;
            typeTrieLimit += typeTrieUnit;            
        } else {
            typeTrieCursor = typeTrie[pos];
        }
      
        //System.err.println(TypeMask.methodTypeName[type<<1]);
        
        //TODO: do something with the type?

        
        resetFieldSum();
    }

    @Override
    public void frameSwitch() {
        // PRINT REPORT
        
        printRecursiveReport(0,"");
        
        
        int j = typeTrie.length;
        while (--j>=0) {
            typeTrie[j]=0;
        }
    }

    
    public void printRecursiveReport(int pos, String tab) {
        
        int i = 16;
        while (--i>=0) {
            int value = typeTrie[pos+i];
            if (value > 0) {                
                if (i==TYPE_EOM) {
    //                    System.err.println(tab+"Count:"+value);
                } else {
                    int type = i<<1;
                    if (type<TypeMask.methodTypeName.length) {
                        String v = (type==TYPE_NULL ? "NULL" : TypeMask.methodTypeName[type]);
                        
      //                  System.err.println(tab+v);
                        printRecursiveReport(value, tab+"  ");    
                    }
                }        
            }
            
        }
        
        
        
        
    }
    
    
}
