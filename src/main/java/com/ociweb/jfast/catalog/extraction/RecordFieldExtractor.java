package com.ociweb.jfast.catalog.extraction;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.zip.GZIPOutputStream;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import com.ociweb.jfast.catalog.generator.CatalogGenerator;
import com.ociweb.jfast.catalog.generator.FieldGenerator;
import com.ociweb.jfast.catalog.generator.ItemGenerator;
import com.ociweb.jfast.catalog.generator.TemplateGenerator;
import com.ociweb.jfast.catalog.loader.ClientConfig;
import com.ociweb.jfast.catalog.loader.TemplateHandler;
import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.primitive.FASTOutput;
import com.ociweb.jfast.primitive.adapter.FASTOutputStream;
import com.ociweb.jfast.stream.FASTRingBufferWriter;
import com.ociweb.jfast.util.Stats;

public class RecordFieldExtractor {

    //High                       1
    private static final int BITS_DECIMAL = 11; //this is the only one that keeps count
    private static final int BITS_SIGN    =    3;
    private static final int BITS_DOT     =      3;
    private static final int BITS_COMMA   =        3;
    private static final int BITS_ASCII   =          3;
    private static final int BITS_OTHER   =            3;   
    
    private static final int SHIFT_OTHER   = 0;
    private static final int SHIFT_ASCII   = BITS_OTHER+SHIFT_OTHER;
    private static final int SHIFT_COMMA   = BITS_ASCII+SHIFT_ASCII;
    private static final int SHIFT_DOT     = BITS_COMMA+SHIFT_COMMA;
    private static final int SHIFT_SIGN    = BITS_DOT+SHIFT_DOT;
    private static final int SHIFT_DECIMAL = BITS_SIGN+SHIFT_SIGN;
    
    private static final int SATURATION_MASK = (1 << (BITS_SIGN + BITS_DOT + BITS_COMMA + BITS_ASCII + BITS_OTHER))-1;
    
        
    private static final int ONE_DECIMAL = 1<<SHIFT_DECIMAL; //0010 0000 0000 0000
    private static final int ONE_SIGN    = 1<<SHIFT_SIGN;    //0000 0100 0000 0000
    private static final int ONE_DOT     = 1<<SHIFT_DOT;     //0000 0000 1000 0000
    private static final int ONE_COMMA   = 1<<SHIFT_COMMA;   //0000 0000 0001 0000
    private static final int ONE_ASCII   = 1<<SHIFT_ASCII;   //0000 0000 0000 0100
    private static final int ONE_OTHER   = 1<<SHIFT_OTHER;   //0000 0000 0000 0001
               
    private static final int   ACCUM_MASK = (((1<<(BITS_DECIMAL-1))-1)<<SHIFT_DECIMAL) |
                                    (((1<<(BITS_SIGN-1))-1)<<SHIFT_SIGN) |
                                    (((1<<(BITS_DOT-1))-1)<<SHIFT_DOT) |
                                    (((1<<(BITS_COMMA-1))-1)<<SHIFT_COMMA) |
                                    (((1<<(BITS_ASCII-1))-1)<<SHIFT_ASCII) |
                                    (((1<<(BITS_OTHER-1))-1)<<SHIFT_OTHER);
    
    public static final int TYPE_UINT = TypeMask.IntegerUnsigned>>1; //  0
    public static final int TYPE_SINT = TypeMask.IntegerSigned>>1;   //  1
    public static final int TYPE_ULONG = TypeMask.LongUnsigned>>1;   //  2
    public static final int TYPE_SLONG = TypeMask.LongSigned>>1;     //  3
    public static final int TYPE_ASCII = TypeMask.TextASCII>>1;      //  4 
    public static final int TYPE_BYTES = TypeMask.TextUTF8>>1;       //  5 
    public static final int TYPE_DECIMAL = TypeMask.Decimal>>1;      //  6
    public static final int TYPE_NULL = 7;//no need to use BYTE_ARRAY, its the same as UTF8
    //NOTE: more will be added here for group and sequence once JSON support is added
    private static final int TYPE_EOM = 15;
    
    //                                              
    private static final int[] SUPER_TYPE = new int[]{
    	                        TYPE_ULONG,   //FROM TYPE_UINT
    	                        TYPE_SLONG,   //FROM TYPE_SINT
    	                        TYPE_DECIMAL, //FROM TYPE_ULONG
    	                        TYPE_DECIMAL, //FROM TYPE_SLONG
    	                        TYPE_BYTES,   //FROM TYPE_ASCII 
    	                        TYPE_BYTES,   //CANT CONVERT, THIS IS EVERYTHING
    	                        TYPE_ASCII,   //FROM TYPE_DECIMAL
    	                        TYPE_NULL,    //CANT CONVERT, THIS IS NOTHING
    };
    
    private final int[] accumValues;
    
    //TODO: need to trim leading white space for decision
    //TODO: need to keep leading real char for '0' '+' '-'
    
    //TODO: null will be mapped to default bytearray null?
    //TODO: next step stream this data using next visitor into the ring buffer and these types.

    //Need flag to turn on that feature
    ///TODO: convert short byte sequences to int or long
    ///TODO: treat leading zero as ascii not numeric.
    
    private int         activePostDotCount;
    private int         activeSum;
    private int         activeLength;
    private boolean     activeQuote;
   
    private Stats       histPostDotCount = new Stats(60,7,0,15);
    
    //16 possible field types
    
    private static final int   typeTrieUnit = 16;
    private static final int   typeTrieArraySize = 1<<20; //1M
    private static final int   typeTrieArrayMask = typeTrieArraySize-1;
    
    private static final int   OPTIONAL_SHIFT = 30;
    private static final int   OPTIONAL_FLAG  = 1<<OPTIONAL_SHIFT;
    private static final int   OPTIONAL_LOW_MASK  = (1<<(OPTIONAL_SHIFT-1))-1;
    
    private static final int   CATALOG_TEMPLATE_ID = 0;
    
    private final int[] typeTrie = new int[typeTrieArraySize];
    private int         typeTrieCursor; 
    private int         typeTrieCursorPrev;
    
    private int         nullCount; //TODO: expand to the other types and externalize rules to an interface
    private int         firstField = -1;
    private int         firstFieldLength = -1;
    private int         utf8Count;
    private int         asciiCount;
    
    private int         typeTrieLimit = typeTrieUnit;
    
    long totalRecords;
    long tossedRecords;
    
    byte[] catBytes;
    
    
    private RecordFieldValidator recordValidator = RecordFieldValidator.ALL_VALID;
    
    
    //  //TODO: add string literals to be extracted by tokenizer
    int reportLimit = 0; //turn off the debug feature by setting this to zero.

    
    public RecordFieldExtractor(RecordFieldValidator recordValidator) {
    	
    	this.recordValidator = recordValidator; 
        
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
        restToRecordStart();
        
    }
    
    MappedByteBuffer tempBuffer;
    int tempPos;
    
    public void appendContent(MappedByteBuffer mappedBuffer, int pos, int limit, boolean contentQuoted) {
   
        tempBuffer = mappedBuffer;
        tempPos = pos;
        
        activeQuote |= contentQuoted;
        
        int i = (limit-pos);
        activeLength+=i;
        while (--i>=0) {
            byte b = mappedBuffer.get(pos+i);
            int x = activeSum + accumValues[0xFF&b];    
            
            activePostDotCount += (ONE_DOT&x);  //may need to use ONE_COMMA FOR UK
            
            activeSum = (x|(((x>>1) & ACCUM_MASK) & SATURATION_MASK))&ACCUM_MASK;  
            
            
        }       
    }

    
    public void appendNewRecord(int startPos) {       

        boolean isValid = recordValidator.isValid(nullCount,utf8Count,asciiCount,firstFieldLength,firstField); 
        
        if (isValid) {                        
            totalRecords++;
            
            int total =  ++typeTrie[typeTrieCursor+TYPE_EOM];
            if (total<=reportLimit) {
                if (tempBuffer.position()-startPos<200) { 
                    
                    System.err.println("example "+total+" for :"+(typeTrieCursor+TYPE_EOM));
                    
                    byte[] dst = new byte[tempBuffer.position()-startPos];
                    ByteBuffer x = tempBuffer.asReadOnlyBuffer();
                    x.position(startPos);
                    x.limit(tempBuffer.position());
                    x.get(dst, 0, dst.length);
                    System.err.println(new String(dst));     
                } else {
                    System.err.println(startPos+" to "+tempBuffer.position());
                }
                
            }
        } else {
            tossedRecords++;
        }
        
        restToRecordStart();
        

        
    }

    public void appendNewField() {
        
        if (firstField<0) {
            firstFieldLength = activeLength;
        }
        
        int type = extractType();
        resetFieldSum(); //TODO: not a  good place for this, side effect.
        
        if (TYPE_NULL == type) {
            nullCount++;
        }
        if (TYPE_BYTES == type) {
            utf8Count++;
        }
        if (TYPE_ASCII == type) {
            asciiCount++;
        }
        if (firstField<0) {
            firstField = type;
        }
        
        
        if (TYPE_UINT == type) {
            //switch up to long if it is already in use
            if (typeTrie[TYPE_ULONG+typeTrieCursor]!=0) {
                type = TYPE_ULONG;
            }            
        }
        
        if (TYPE_SINT == type) {
            //switch up to long if it is already in use
            if (typeTrie[TYPE_SLONG+typeTrieCursor]!=0) {
                type = TYPE_SLONG;
            }            
        }
        
        //store type into the Trie to build messages.
        
        int pos = typeTrieCursor+type;
                
        
        if (typeTrie[pos]==0) {
            //create new position          
            typeTrieCursor = typeTrie[pos] = typeTrieLimit;
            typeTrieLimit += typeTrieUnit;
        } else {
            typeTrieCursor = OPTIONAL_LOW_MASK&typeTrie[pos];
        }
    }
    
    public int moveNextField() {
        int type = extractType();        
        resetFieldSum(); //TODO: not a  good place for this, side effect.
        
        typeTrieCursorPrev = typeTrieCursor;
        typeTrieCursor = OPTIONAL_LOW_MASK&typeTrie[typeTrieCursor+type];  
        return type;
    }

    /**
     * Only called after the record and types have been established from the first pass.
     * This is needed to map the raw extract types to the final determined types after
     * the full analysis at the end of the last iteration.
     * 
     * @param type
     * @return
     */
	public int convertRawTypeToSpecific(int type) {
		
		//back up to parent cursor location
		typeTrieCursor=typeTrieCursorPrev;
		
		//if type is null???
        if (TYPE_NULL == type) {            
            //select the optional type instead of null
            int i = typeTrieUnit;
            while (--i>=0) {
                if (TYPE_NULL != i) {                    
                    if (0 != (OPTIONAL_FLAG & ( typeTrie[typeTrieCursor+i]))) { 
                        
                        typeTrieCursor = OPTIONAL_LOW_MASK&typeTrie[typeTrieCursor+i];  
                    	return i; 
                    }                
                }                
            } 
         //   int[] temp = Arrays.copyOfRange(typeTrie, typeTrieCursor, typeTrieCursor+typeTrieUnit);
           // System.err.println("non found "+Arrays.toString(temp));
        } else {
        	//if the type is not found then bump it up until it is found
        	while (totalCount(typeTrieCursor+type, typeTrie)==0) {
        		//bump up type
        		int newType = SUPER_TYPE[type];  
                if (type==newType) {
                	break;
                }
        		type = newType;      		        		
        	}
        }
        typeTrieCursor = OPTIONAL_LOW_MASK&typeTrie[typeTrieCursor+type];  
        return type;
	}
    
    private int extractType() {
        assert(activeLength>=0);
                
        //split fields.
        int otherCount = ((1<<BITS_OTHER)-1)&(activeSum>>SHIFT_OTHER);
        int decimalCount = ((1<<BITS_DECIMAL)-1)&(activeSum>>SHIFT_DECIMAL);
        int asciiCount = ((1<<BITS_ASCII)-1)&(activeSum>>SHIFT_ASCII);
        int signCount = ((1<<BITS_SIGN)-1)&(activeSum>>SHIFT_SIGN);
        
        //NOTE: swap these two assignments for British vs American numbers
        int dotCount = ((1<<BITS_DOT)-1)&(activeSum>>SHIFT_DOT);
        int commaCount = ((1<<BITS_COMMA)-1)&(activeSum>>SHIFT_COMMA);
                
//        if (commaCount>0) {
//            System.err.println("did not expect any commas");
//        }                     
        
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
                        
                        //        activePostDotCount
                        //TODO: need histogram of the values ? then use percentile to pick
                        int decAfterDot = activePostDotCount>>SHIFT_DOT;
                        histPostDotCount.sample(decAfterDot);
                        
                       
                        
//                        if (decAfterDot>maxPostDotCount) {
//                            maxPostDotCount = decAfterDot;
//                            System.err.println("xxx: "+maxPostDotCount);
//                        }
                                    
                        //System.err.println(decAfterDot);
                        
                        
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
                                type = TYPE_UINT; //TODO: may need to bump up to long if that is all we find when building records.
                            } else {
                                //signed
                                type = TYPE_SINT;
                            }                            
                        }
                    }                
                }           
            }
        }

        return type;
    }
    
    
    private void resetFieldSum() {
        activeLength = 0;
        activeSum = 0;
        activePostDotCount = 0;
        activeQuote = false;
    }
    
    public void restToRecordStart() {
        nullCount = 0;
        utf8Count = 0;
        asciiCount = 0;
        typeTrieCursor = 0;
        firstField = -1;
    }
    
    
    public void printStatus() {
        System.err.println(histPostDotCount);
    }
    //if all zero but the null then do recurse null otherwise not.
    
    public void printRecursiveReport(int pos, String tab) {
        
        int i = typeTrieUnit;
        boolean noOutput = true;
        while (--i>=0) {
            int value = OPTIONAL_LOW_MASK&typeTrie[pos+i];
            if (value > 0) {                
                if (i==TYPE_EOM) {
                        System.err.print(tab+"Count:"+value+" at "+(pos+i)+"\n");
                        noOutput = false;
                } else {
                    if (totalCount(value, typeTrie)>0) {
                    
                        
                        int type = i<<1;
                        if (type<TypeMask.methodTypeName.length) {                    
                            
                            
                            String v = (i==TYPE_NULL ? "NULL" : TypeMask.methodTypeName[type]);
                            
                            if ((OPTIONAL_FLAG&typeTrie[pos+i])!=0) {
                                v = "Optional"+v;
                            }
                            noOutput = false;
                            
                            System.err.println(tab+v);
                            printRecursiveReport(value, tab+"     ");    
                        }
                    }
                }        
            }
        }        
        if (noOutput) {
            System.err.print(tab+"Count:ZERO\n");
        }
    }
    

    
    private static int totalCount(int value, int[] typeTrie) {
        int sum = 0;
        int i = typeTrieUnit;
        while (--i>=0) {
            int temp = OPTIONAL_LOW_MASK&typeTrie[value+i];
            if (TYPE_EOM==i) {
                sum += temp;                
            } else {
                if (temp>0) {
                    sum += totalCount(temp, typeTrie);
                }
            }
        }
        return sum;
    }


    void mergeOptionalNulls(int pos) {
        
        int i = typeTrieUnit;
        while (--i>=0) {
            int value = OPTIONAL_LOW_MASK&typeTrie[pos+i];
            if (value > 0) {                
                if (i!=TYPE_EOM && totalCount(value, typeTrie)>0) {
                    mergeOptionalNulls(value);
    
                    //finished call for this position i so it can removed if needed
                    //after merge on the way up also ensure we are doing the smallest parts first then larger ones
                    //and everything after this point is already merged.
                    
                    int lastNonNull = lastNonNull(pos, typeTrie, typeTrieUnit);
                    
                    if (lastNonNull>=0 && TYPE_NULL==i) {
                    
                       // System.err.println("found one "+lastNonNull);
                    
                        //check if there is another non-null field
                        //if there is more than 1 field with the null NEVER collapse because we don't know which path to which it belongs.
                        //we will produce 3 or more separate templates and they will be resolved by the later consumption stages
                        if (lastNonNull(pos, typeTrie, lastNonNull)<0) {
                            
                            int nullPos = value;
                            int thatPosDoes = OPTIONAL_LOW_MASK&typeTrie[pos+lastNonNull];
                                                        
                            //if recursively all of null pos is contained in that pos then we will move it over.                            
                            if (contains(nullPos,thatPosDoes)) {
                                //since the null is a full subset add all its counts to the larger
                                sum(nullPos,thatPosDoes);                                
                                //flag this type as optional
                                typeTrie[pos+lastNonNull] |= OPTIONAL_FLAG;
                            }
                        }
                    }
                }        
            }            
        }        
    }
    
   
     
    
    //If two numeric types are found and one can fit inside the other then merge them.
    void mergeNumerics(int pos) { //TODO: rename as general merge
        
        int i = typeTrieUnit;
        while (--i>=0) {
            int value = OPTIONAL_LOW_MASK&typeTrie[pos+i];
            if (value > 0) {                
                if (i!=TYPE_EOM  && totalCount(value,typeTrie)>0) {
                    mergeNumerics(value);
    
                    //finished call for this position i so it can removed if needed
                    //after merge on the way up also ensure we are doing the smallest parts first then larger ones
                    //and everything after this point is already merged.
                    
                    //uint to ulong to decimal
                    //sint to slong to decimal

                    mergeTypes(pos, i, value, TYPE_ULONG, TYPE_UINT); //UINT  into ULONG
                    mergeTypes(pos, i, value, TYPE_SLONG, TYPE_SINT); //SINT  into SLONG
                    
                    //TOOD: in the future may want to only do the merge if the sub type is only a small count of instances relative to the super type.         
                    mergeTypes(pos, i, value, TYPE_DECIMAL, TYPE_SINT); //SLONG into DECIMAL
                    mergeTypes(pos, i, value, TYPE_DECIMAL, TYPE_UINT); //SLONG into DECIMAL
                    mergeTypes(pos, i, value, TYPE_DECIMAL, TYPE_SLONG); //SLONG into DECIMAL
                    mergeTypes(pos, i, value, TYPE_DECIMAL, TYPE_ULONG); //SLONG into DECIMAL
                    
                }        
            }            
        }        
    }

    //TODO: this is not recursive and we need it do a deep contains and sum so we must extract the subset rules.
    private void mergeTypes(int pos, int i, int value, int t1, int t2) {
        int thatPosDoes = OPTIONAL_LOW_MASK&typeTrie[pos+t1];
        if (thatPosDoes>0 && t2==i) {
                                   
                
                //if recursively all of null pos is contained in that pos then we will move it over.                            
                if (contains(value,thatPosDoes)) {
                    //since the null is a full subset add all its counts to the rlarger
                    sum(value,thatPosDoes);   
                }

        }
    }
    
   
    
    private int targetType(int i, int targetset) {
        if (TYPE_NULL==i) {
            //if only one type is optional then we can represent this null as that type.
            //if there is more than one then we must not combine them until or if the types are merged first
            int q = typeTrieUnit;
            int result = -1;
            while (--q>=0) {
                if (TYPE_EOM!=q &&
                    TYPE_NULL!=q && 
                    totalCount(OPTIONAL_LOW_MASK&typeTrie[targetset+q], typeTrie)>0  &&
                    (0!=  (OPTIONAL_FLAG&typeTrie[targetset+q]))) {
                    
                        if (result<0) {
                            result = q;
                        } else {
                            return -1;
                        }  
                        
                }                
            }
            return result;
            
        } else {
        
        
            //if own kind is not found check for the simple super
            if (TYPE_UINT==i) {//TODO: EXPAND FOR SUPPORT OF SIGNED
                if (0==(OPTIONAL_LOW_MASK&typeTrie[targetset+TYPE_ULONG])) {
                    if (0==(OPTIONAL_LOW_MASK&typeTrie[targetset+TYPE_DECIMAL])) {
                        return -1;
                    } else {
                        return TYPE_DECIMAL;
                    }
                } else {
                    return TYPE_ULONG;
                }
            } else {
                
                if (TYPE_ULONG==i) {//TODO: EXPAND FOR SUPPORT OF SIGNED
                    if (0==(OPTIONAL_LOW_MASK&typeTrie[targetset+TYPE_DECIMAL])) {
                        return -1;
                    } else {
                        return TYPE_DECIMAL;
                    }
                } else {
                    return -1;
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
                if (0!=(OPTIONAL_LOW_MASK&typeTrie[subset+i]) && totalCount(OPTIONAL_LOW_MASK&typeTrie[subset+i], typeTrie)>0) { 
                    int j = i;
                    if (0==(OPTIONAL_LOW_MASK&typeTrie[targetset+i]) && totalCount(OPTIONAL_LOW_MASK&typeTrie[targetset+i], typeTrie)>0) {
                        
                        j = targetType(i,targetset);
                        if (j<0) {
                            return false;//TODO: is this giving up too early!!!?? only if null?
                        }                                            
                        
                    }                           
                    if (!contains(OPTIONAL_LOW_MASK&typeTrie[subset+i],OPTIONAL_LOW_MASK&typeTrie[targetset+j])  ) {
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
            int j = i;
            //exclude this type its only holding the count
            if (TYPE_EOM!=i) {
                
                if (0!=(OPTIONAL_LOW_MASK&typeTrie[subset+i]) && totalCount(OPTIONAL_LOW_MASK&typeTrie[subset+i], typeTrie)>0 ) {
                    
                    if (0==(OPTIONAL_LOW_MASK&typeTrie[targetset+i]) && totalCount(OPTIONAL_LOW_MASK&typeTrie[targetset+i], typeTrie)>0) {
                        
                        
                        j = targetType(i,targetset);
                        if (j<0) {
                            return false;
                        }
                        
                    }
                    
                    if (!sum(OPTIONAL_LOW_MASK&typeTrie[subset+i],OPTIONAL_LOW_MASK&typeTrie[targetset+j])  ) {
                        return false;
                    }
                }

                //don't loose the optional flag from the other branch if it is there                
                typeTrie[targetset+j]  = (OPTIONAL_FLAG & (typeTrie[subset+i] | typeTrie[targetset+j])) |
                                         (OPTIONAL_LOW_MASK&(typeTrie[targetset+j]));
            } else {
                //everything matches to this point so add the inner into the outer
                typeTrie[targetset+j]  = (OPTIONAL_LOW_MASK & (typeTrie[subset+i] + typeTrie[targetset+j]));
                typeTrie[subset+i] = 0;//clear out old value so we do not double count
            }
        }                    
        return true;
    }
    
    
    private static int lastNonNull(int pos, int[] typeTrie, int startLimit) {
        int i = startLimit;
        while (--i>=0) {
            if (TYPE_NULL!=i && TYPE_EOM!=i) {
                int v =OPTIONAL_LOW_MASK&typeTrie[pos+i];
                if (0!=v && totalCount(v, typeTrie)>0) {
                    return i;
                }
            }            
        }
        return -1;
    }
    
    
    private  void catalog(int pos, StringBuilder target, ItemGenerator[] buffer, int idx, String exponent) {
        
        int i = typeTrieUnit;
        while (--i>=0) {
            int raw = typeTrie[pos+i];
            int value = OPTIONAL_LOW_MASK&raw;
            int optionalBit = 1&(raw>>OPTIONAL_SHIFT);
            
            if (value > 0) {                
                if (i==TYPE_EOM) {
                        
                    String name=""+pos;
                    int id=pos;
                    
                    boolean reset=false;
                    String dictionary=null;
                    
                    TemplateGenerator.openTemplate(target, name, id, reset, dictionary);
                    
                    int j = 0;
                    while (j<idx) {
                        buffer[j].appendTo("    ", target);
                        j++;
                    }
                        
                    TemplateGenerator.closeTemplate(target);
                    
                } else {
                    
                    int type = i<<1;
                    if (type<TypeMask.methodTypeName.length) {        
                                                
                        if (i==TYPE_NULL) {
                            type = TypeMask.TextUTF8Optional;
                        } else {
                            type = type|optionalBit;
                            
                        }
                        
                        //pos + type is used because it will never collide
                        int id = pos+i; //pos skips by 16's so there is room for the i
                        String name = ""+id;
                        boolean presence = 1==optionalBit;
                        int operator = OperatorMask.Field_None;
                        String initial = null;
                    
                        
                        if (TypeMask.Decimal==type || TypeMask.DecimalOptional==type ) {
                            //TODO: this uses a constant exponent for all values, we will want to make the settable by field position or name
                            buffer[idx] = new FieldGenerator(name,id,presence,type,OperatorMask.Field_Constant,operator,exponent,initial);  
                        } else {
                            buffer[idx] = new FieldGenerator(name,id,presence,type,operator,initial);                                      
                        }
                        
                        catalog(value, target, buffer, idx+1, exponent);    
                    }
                }        
            }
        }        

    }
    
    
    public String buildCatalog(boolean withCatalogSupport) {       
        
        StringBuilder target = new StringBuilder(1024);
        target.append(CatalogGenerator.HEADER);
               
        
        
        //TODO: this is a global value but we really need 1. a value per field 2. user input percentile values
        int exponent = globalExponent();
        //TODO: this is using a constant exponent for the decimals, that may not be what we need for other situations
        

        
        
        ItemGenerator[] buffer = new ItemGenerator[64];        
        catalog(0,target,buffer,0, String.valueOf(exponent));
                
        if (withCatalogSupport) {
            addTemplateToHoldTemplates(target);
        }
        
        
        target.append(CatalogGenerator.FOOTER);
        return target.toString();      
        
        
    }

    public void addTemplateToHoldTemplates(StringBuilder target) {
        String name="catalog";
        int id=CATALOG_TEMPLATE_ID;
        
        boolean reset=true;
        String dictionary="global";

        TemplateGenerator.openTemplate(target, name, id, reset, dictionary);
        
        
        int catId = 100;
        String catName = ""+catId;
        boolean presence = false;
        int operator = OperatorMask.Field_None;
        String initial = null;
        int type = TypeMask.ByteArray;
        
        FieldGenerator fg;
        fg = new FieldGenerator(catName,catId,presence,type,operator,initial);  
        fg.appendTo("    ", target);            
        
        TemplateGenerator.closeTemplate(target);
    }
    
    public byte[] catBytes(ClientConfig clientConfig) {
        String catalog = buildCatalog(true);
        
        clientConfig.setCatalogTemplateId(CATALOG_TEMPLATE_ID);
        
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            GZIPOutputStream gZipOutputStream = new GZIPOutputStream(baos);
            FASTOutput output = new FASTOutputStream(gZipOutputStream);
            
            SAXParserFactory spfac = SAXParserFactory.newInstance();
            SAXParser sp = spfac.newSAXParser();
            InputStream stream = new ByteArrayInputStream(catalog.getBytes(StandardCharsets.UTF_8));           
            
            TemplateHandler handler = new TemplateHandler(output, clientConfig);            
            sp.parse(stream, handler);
    
            handler.postProcessing();
            gZipOutputStream.close();            
            
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        byte[] catBytes = baos.toByteArray();
        return catBytes;
    }

    public void memoizeCatBytes() {
    //    System.err.println(buildCatalog(true));
        catBytes = catBytes(new ClientConfig());
        
    }
    
    public byte[] getCatBytes() {
        return catBytes;
    }

    public int globalExponent() {
        return (int)histPostDotCount.valueAtPercent(.999d);
    }

    
    
}
