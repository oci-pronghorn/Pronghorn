package com.ociweb.pronghorn.components.ingestion.metaMessageUtil;

import java.nio.ByteBuffer;

import com.ociweb.pronghorn.ring.token.TypeMask;

public class TypeExtractor {
	
	private final boolean forceASCII;//disables support for UTF-8, and use ASCII in its place.
	
    //TODO add count of quotes to be checked at start and end.
	//TODO simplify saturation so 3 bits are no longer required
    
    private static final int BITS_DECIMAL  = 8; //above 18 and we use ASCII instead so only needs 5 bits for count and saturation
    private static final int BITS_SIGN_NEG =  3; //TODO: need count of these for SSN/DATE parse
    private static final int BITS_SIGN_POS =    3;
    private static final int BITS_DOT      =      3;
    private static final int BITS_COMMA    =        3;
    private static final int BITS_ASCII    =          3;
    private static final int BITS_OTHER    =            3;   
    
    private static final int SHIFT_OTHER   = 0;
    private static final int SHIFT_ASCII   = BITS_OTHER+SHIFT_OTHER;
    private static final int SHIFT_COMMA   = BITS_ASCII+SHIFT_ASCII;
    public static final int SHIFT_DOT     = BITS_COMMA+SHIFT_COMMA;
    private static final int SHIFT_SIGN_POS = BITS_DOT+SHIFT_DOT;
    private static final int SHIFT_SIGN_NEG = BITS_SIGN_POS+SHIFT_SIGN_POS;
    private static final int SHIFT_DECIMAL = SHIFT_SIGN_NEG+BITS_SIGN_NEG;
    
    static final int SATURATION_MASK = (1 << (SHIFT_SIGN_NEG + SHIFT_SIGN_POS + BITS_DOT + BITS_COMMA + BITS_ASCII + BITS_OTHER))-1;
    
        
    private static final int ONE_DECIMAL  = 1<<SHIFT_DECIMAL; 
    private static final int ONE_SIGN_NEG = 1<<SHIFT_SIGN_NEG; 
    private static final int ONE_SIGN_POS = 1<<SHIFT_SIGN_POS;   
    static final int         ONE_DOT      = 1<<SHIFT_DOT;     
    private static final int ONE_COMMA    = 1<<SHIFT_COMMA;   
    private static final int ONE_ASCII    = 1<<SHIFT_ASCII;  
    private static final int ONE_OTHER    = 1<<SHIFT_OTHER;  
               
    static final int   ACCUM_MASK = (((1<<(BITS_DECIMAL-1))-1)<<SHIFT_DECIMAL) |
                                    (((1<<(BITS_SIGN_NEG-1))-1)<<SHIFT_SIGN_NEG) |
                                    (((1<<(BITS_SIGN_POS-1))-1)<<SHIFT_SIGN_POS) |
                                    (((1<<(BITS_DOT-1))-1)<<SHIFT_DOT) |
                                    (((1<<(BITS_COMMA-1))-1)<<SHIFT_COMMA) |
                                    (((1<<(BITS_ASCII-1))-1)<<SHIFT_ASCII) |
                                    (((1<<(BITS_OTHER-1))-1)<<SHIFT_OTHER);
		
	
	public static final int[] accumValues;
	static {
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
        accumValues[(int)'+'] = ONE_SIGN_POS;
        accumValues[(int)'-'] = ONE_SIGN_NEG;        
        accumValues[(int)'.'] = ONE_DOT;
        accumValues[(int)','] = ONE_COMMA; //required when comma is not the delimiter to support thousands marking in US english
	}
	

	public int activePosDotCount;
	public int activePosSignCount;
	public int activeSum;
	public int activeLength;
	public long activeFieldLong;

	public static final int TYPE_NULL = 7;//no need to use BYTE_ARRAY, its the same as UTF8

	public static final int TYPE_DECIMAL = TypeMask.Decimal>>1;      //  6
	public static final int TYPE_BYTES = TypeMask.TextUTF8>>1;       //  5 
	public static final int TYPE_ASCII = TypeMask.TextASCII>>1;      //  4 
	public static final int TYPE_SLONG = TypeMask.LongSigned>>1;     //  3
	public static final int TYPE_ULONG = TypeMask.LongUnsigned>>1;   //  2
	public static final int TYPE_SINT = TypeMask.IntegerSigned>>1;   //  1
	public static final int TYPE_UINT = TypeMask.IntegerUnsigned>>1; //  0
                                    
                                    
	public static final long[] POW_10;
    static {
    	int max = 19;
    	POW_10 = new long[max];
    	int j = 0;
    	long total=1;
    	while (j<max) {
    		POW_10[j++]=total;
    		total *= 10l;
    	}    	
    }
	
	public TypeExtractor(boolean forceASCII) {
		this.forceASCII = forceASCII;
		
		resetFieldSum(this);
	}

	public static void appendContent(TypeExtractor typeExtractor, ByteBuffer mappedBuffer, int pos, int limit) {
	    assert(limit>=pos);
	    typeExtractor.activeLength += (limit-pos);
	        
	    int j = pos;
	    do {
	        //skips all non quoted leading white space 
	    }  while (' '==mappedBuffer.get(j) && ++j<limit );
	    
	    long accumValue = typeExtractor.activeFieldLong;
	    while (j < limit) {
	        accumValue = sampleSingleByte(typeExtractor, accumValue, mappedBuffer.get(j++));
	    }       
	    typeExtractor.activeFieldLong = accumValue;
	    
	}
	
	public static void appendContent(TypeExtractor typeExtractor, byte[] data, int pos, final int limit) {
		assert(limit>=pos);
	    typeExtractor.activeLength += (limit-pos);

	    int j = pos;
	    do {
	        //skips all non quoted leading white space 
	    }  while (' '==data[j] && ++j<limit);

	    long accumValue = typeExtractor.activeFieldLong;
		while (j < limit) {	    
		    accumValue = sampleSingleByte(typeExtractor, accumValue, data[j++]);
		}       
		typeExtractor.activeFieldLong = accumValue;
	    
	}

	private static long sampleSingleByte(TypeExtractor typeExtractor, long accumValue, byte b) {
		//Total up all the counts of all the char types that we want to track in one single addition
		  int x = typeExtractor.activeSum;
		  //if a single dot was found continue to count this value until the end is reached, required for decimal placement.
		  
		  typeExtractor.activePosDotCount += (ONE_DOT&x);  //TODO: B, may need to use ONE_COMMA FOR UK
		  typeExtractor.activePosSignCount += (ONE_SIGN_NEG&x); //TODO: B, these two could be combined
		  
		  //saturation logic to ensure fields to not roll into each other.
		  x += TypeExtractor.accumValues[0xFF&b];
		  typeExtractor.activeSum = (x|(((x>>1) & ACCUM_MASK) & SATURATION_MASK))&ACCUM_MASK;
		
		  x = (b & 0x0F);
		  if (x<10) {//skips all the decorations
	 		accumValue = (accumValue*10)+x;
	  	  }
		  return accumValue;
	}

	public static int extractType(TypeExtractor typeExtractor) {
	        assert(typeExtractor.activeLength>=0) : "discovered active length of "+typeExtractor.activeLength;
	                
	        //split fields.
	        int otherCount = ((1<<BITS_OTHER)-1)&(typeExtractor.activeSum>>SHIFT_OTHER);
	        int decimalCount = ((1<<BITS_DECIMAL)-1)&(typeExtractor.activeSum>>SHIFT_DECIMAL); 
	        int asciiCount = ((1<<BITS_ASCII)-1)&(typeExtractor.activeSum>>SHIFT_ASCII);
	        int signCountPos = ((1<<BITS_SIGN_POS)-1)&(typeExtractor.activeSum>>SHIFT_SIGN_POS);
	        int signCountNeg = ((1<<BITS_SIGN_NEG)-1)&(typeExtractor.activeSum>>SHIFT_SIGN_NEG);
	        
	        //NOTE: swap these two assignments for British vs American numbers
	        int dotCount = ((1<<BITS_DOT)-1)&(typeExtractor.activeSum>>SHIFT_DOT);
	        int commaCount = ((1<<BITS_COMMA)-1)&(typeExtractor.activeSum>>SHIFT_COMMA);
	                
	//        if (commaCount>0) {
	//            System.err.println("did not expect any commas");
	//        }                     
	        
	        //apply rules to determine field type
	        if (typeExtractor.activeLength==0) {
	            //null field
	            return TypeExtractor.TYPE_NULL; 
	        } else {        
	            if (otherCount>0) {
	            	
	                //utf8 or byte array
	                return typeExtractor.forceASCII? TypeExtractor.TYPE_ASCII : TypeExtractor.TYPE_BYTES;
	            } else { 
	            	//TODO: what if sign is in odd position!
	                if (asciiCount>0 || signCountPos>1 || signCountNeg>1 || dotCount>1 || decimalCount>18) { //NOTE: 18 could be optimized by reading first digit
	                	//ascii text
	                    return TypeExtractor.TYPE_ASCII;
	                } else {
	                    if (dotCount==1) {
	                        //decimal                         
	                        return TypeExtractor.TYPE_DECIMAL;                        
	                        
	                    } else {  
	                        //no dot
	                        if (decimalCount>9) { //NOTE: 9 could be optimized by reading first digit
	                            //long
	                            if (signCountNeg==0) {
	                                //unsigned
	                                return TypeExtractor.TYPE_ULONG;
	                            } else {
	                                //signed
	                                return TypeExtractor.TYPE_SLONG;
	                            }
	                        } else {
	                            //int
	                            if (signCountNeg==0) {
	                                //unsigned
	                                return TypeExtractor.TYPE_UINT; 
	                            } else {
	                                //signed
	                                return TypeExtractor.TYPE_SINT;
	                            }                            
	                        }
	                    }                
	                }           
	            }
	        }
	    }

	public static void resetFieldSum(TypeExtractor typeExtractor) {
	    typeExtractor.activeLength = 0;
	    typeExtractor.activeSum = 0;
	    typeExtractor.activePosDotCount = 0;
	    typeExtractor.activePosSignCount = 0;
	    typeExtractor.activeFieldLong = 0;
	}

	public static int decimalPlaces(TypeExtractor typeExtractor) {
	    return typeExtractor.activePosDotCount>>SHIFT_DOT;
	}
	
	public static int signMult(TypeExtractor typeExtractor) {
		int signCountNeg = ((1<<BITS_SIGN_NEG)-1)&(typeExtractor.activeSum>>SHIFT_SIGN_NEG);
		return (signCountNeg==1? -1 :1);
	}
	
	
}