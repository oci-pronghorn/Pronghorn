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
    
    static final int   typeTrieUnit = 16;
    static final int   typeTrieSize = 1<<20; //1M
    static final int   typeTrieMask = typeTrieSize-1;
    static final int   OPTIONAL_FLAG =  1<<30;
    
    final int[] typeTrie = new int[typeTrieSize];
    int         typeTrieCursor; 
    int         typeTrieLimit = typeTrieUnit;
    
    
    public FieldTypeVisitor() {
        
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
            int x = activeSum + accumValues[0xFF&b];
            
            int y = ((x>>1) & ACCUM_MASK) & 0x7FFF;
            
            activeSum = (x|y)&ACCUM_MASK;
            
            //activeSum = (activeSum + accumValues[0xFF&b]) & ACCUM_MASK;            
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
        
   //     System.err.println(Integer.toBinaryString(activeSum));
        
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
    //    System.err.println(v+" from "+temp.toString());
        
        //it points to next which may end up holding the count.
        
        //if this one is null and one already exists then change it to opional
        //if this null and none then both must
        //both null and real value point to same object!!!!!
        
        int pos = typeTrieCursor+type;
        if (typeTrie[pos]==0) {
            //create new position
          
            typeTrieCursor = typeTrie[pos] = typeTrieLimit;
            typeTrieLimit += typeTrieUnit; 

        } else {
            typeTrieCursor = typeTrieMask&typeTrie[pos];
        }
        resetFieldSum();
    }

    @Override
    public void frameSwitch() {
        
        //consolidation rules
        // if only 1 type plus null merge them
        // need to merge target of both together.
        
        mergeOptionalNulls(0);
        
        // PRINT REPORT
        printRecursiveReport(0,"");
        
        
        int j = typeTrie.length;
        while (--j>=0) {
            typeTrie[j]=0;
        }
    }

    
    //if all zero but the null then do recurse null otherwise not.
    
    public void printRecursiveReport(int pos, String tab) {
        
        int i = typeTrieUnit;
        while (--i>=0) {
            int value = typeTrieMask&typeTrie[pos+i];
            if (value > 0) {                
                if (i==TYPE_EOM) {
                        System.err.print(tab+"Count:"+value+"\n");
                } else {
                    int type = i<<1;
                    if (type<TypeMask.methodTypeName.length) {                    
                        
                        
                        String v = (i==TYPE_NULL ? "NULL" : TypeMask.methodTypeName[type]);
                        
                        if ((OPTIONAL_FLAG&typeTrie[pos+i])!=0) {
                            v = "Optional"+v;
                        }
                        
                        
                        System.err.print(tab+v);
                        printRecursiveReport(value, tab+"  ");    
                    }
                }        
            }
            
        }        
    }
    
    public void nav(int pos) {
        
        int i = typeTrieUnit;
        while (--i>=0) {
            int value = typeTrieMask&typeTrie[pos+i];
            if (value > 0) {                
                if (i==TYPE_EOM) {
                    //value contains count
                    
                } else {
                    int type = i<<1;
                    if (type<TypeMask.methodTypeName.length) {                    
                        
                        
                        //String stringType = (i==TYPE_NULL ? "NULL" : TypeMask.methodTypeName[type]);
                        
                        
                        
                        
                        nav(value);    
                    }
                }        
            }
            
        }        
    }
    
    public void mergeOptionalNulls(int pos) {
        
        int i = typeTrieUnit;
        while (--i>=0) {
            int value = typeTrieMask&typeTrie[pos+i];
            if (value > 0) {                
                if (i!=TYPE_EOM) {
                    int type = i<<1;
                    if (type<TypeMask.methodTypeName.length) {      
                        mergeOptionalNulls(value);
                        //finished call for this position i so it can removed if needed
                        //after merge on the way up also ensure we are doing the smallest parts first then larger ones
                        //and everything after this point is already merged.
                        
                        int lastNonNull = lastNonNull(pos, typeTrie, typeTrieUnit);
                        //TODO: if there is more than one will need to add that functionality later by continuing from lastNonNull
                        //TODO: if null is a subset of two different branches then what? stand alone or merge to both?
                        if (lastNonNull>=0 && TYPE_NULL==i) {
                                                    
                            
                            int nullPos = value;
                            int thatPosDoes = typeTrieMask&typeTrie[pos+lastNonNull];
                            
                            //if recursively all of null pos is contained in that pos then we will move it over.                            
                            if (contains(nullPos,thatPosDoes)) {
                                //since the null is a full subset add all its counts to the rlarger
                                sum(nullPos,thatPosDoes);
                                
                                //flag this type as optional
                                typeTrie[thatPosDoes] |= OPTIONAL_FLAG;
                                //delete old null branch
                                typeTrie[nullPos] = 0;
                            }             
                        }
                    }
                }        
            }            
        }        
    }

    private boolean contains(int subset, int targetset) {
        //if all the field in inner are contained in outer
        int i = typeTrieUnit;
        while (--i>=0) {
            //exclude this type its only holding the count
            if (TYPE_EOM!=i) {
                if (0!=(typeTrieMask&typeTrie[subset+i])) {
                    if (0==(typeTrieMask&typeTrie[targetset+i]) || !contains(typeTrieMask&typeTrie[subset+i],typeTrieMask&typeTrie[targetset+i])  ) {
                        return false;
                    }
                }
            }
        }                    
        return true;
    }
    
    private boolean sum(int subset, int targetset) {
        //if all the field in inner are contained in outer
        int i = typeTrieUnit;
        while (--i>=0) {
            //exclude this type its only holding the count
            if (TYPE_EOM!=i) {
                if (0!=(typeTrieMask&typeTrie[subset+i])) {
                    if (0==(typeTrieMask&typeTrie[targetset+i]) || !sum(typeTrieMask&typeTrie[subset+i],typeTrieMask&typeTrie[targetset+i])  ) {
                        return false;
                    }
                }
                //don't loose the optional flag from the other branch if it is there
                typeTrie[targetset+i] |= (OPTIONAL_FLAG & typeTrie[subset+i]);
            } else {
                //everything matches to this point so add the inner into the outer
                typeTrie[targetset+i] += typeTrie[subset+i];
            }
        }                    
        return true;
    }
    
    
    private static int lastNonNull(int pos, int[] typeTrie, int startLimit) {
        int i = startLimit;
        while (--i>=0) {
            if (TYPE_NULL!=i) {
                if (0!=typeTrie[pos+i]) {
                    return i;
                }
            }            
        }
        return -1;
    }
    
    
    
}
