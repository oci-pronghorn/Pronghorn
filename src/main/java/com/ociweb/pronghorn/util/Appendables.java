package com.ociweb.pronghorn.util;

import java.io.IOException;

import com.ociweb.pronghorn.pipe.Pipe;

/**
 * 
 * Garbage free single pass utilities for building up text.
 * The API follows a fluent pattern where every method returns the same Appendable which was passed in.
 * 
 * TODO: as soon we we start building this code for Java 8 we muse use new UncheckedIOException(cause) instead of new RuntimeException();
 * 
 * @author Nathan Tippy
 *
 */
public class Appendables {
    
    private final static char[] hBase = new char[] {'0','1','2','3','4','5','6','7','8','9',
      'a','b','c','d','e','f'};
    
    public static <A extends Appendable> A appendArray(A target, char left, long[] a, char right) {
    	try {
	        if (a != null) {        
	            int iMax = a.length - 1;
	            if (iMax == -1) {
	                target.append(left).append(right);
	                return target;
	            }
	            target.append(left);
	            for (int i = 0; ; i++) {
	                appendValue(target,a[i]);
	                //target.append(Long.toString(a[i]));
	                if (i == iMax)
	                    return (A) target.append(right);
	                target.append(", ");
	            } 
	        } else {
	            target.append("null");
	        
	        }
	        return target;
		} catch (IOException ex) {
			throw new RuntimeException(ex); 
		}
    }

    
    public static <A extends Appendable> A appendArray(A target, byte[] b) {
   		return appendArray(target, '[', b, ']', b.length);
    }
    
    public static <A extends Appendable> A appendArray(A target, char left, byte[] b, char right) {
   		return appendArray(target, left, b, right, b.length);
    }
        
    public static <A extends Appendable> A appendArray(A target, char left, byte[] b, char right, int bLength) {
		try {
		    	if (b != null) {        
		            int iMax = bLength - 1;
		            if (iMax == -1) {
		                target.append(left).append(right);
		                return target;
		            }
		            target.append(left);
		            for (int i = 0; ; i++) {
		                appendValue(target,b[i]);
		                //target.append(Integer.toString(a[i]));
		                if (i == iMax)
		                    return (A) target.append(right);
		                target.append(", ");
		            } 
		        } else {
		            target.append("null");        
		        }
		        return target;
		} catch (IOException ex) {
			throw new RuntimeException(ex); 
		}
    }
    
    public static <A extends Appendable> A appendArray(A target, byte[] b, int offset, int mask, int bLength) {
    	return appendArray(target,'[',b,offset,mask,']',bLength);
    }
    
    public static <A extends Appendable> A appendArray(A target, char left, byte[] b, int offset, int mask, char right, int bLength) {
	      try {
	    	if (b != null) {        
	            int iMax = bLength - 1;
	            if (iMax == -1) {
	                target.append(left).append(right);
	                return target;
	            }
	            target.append(left);
	            for (int i = 0; ; i++) {
	                appendValue(target,b[mask & (i+offset) ]);
	                //target.append(Integer.toString(a[i]));
	                if (i == iMax)
	                    return (A) target.append(right);
	                target.append(", ");
	            } 
	        } else {
	            target.append("null");
	        
	        }
	        return target;
	    } catch (IOException ex) {
			throw new RuntimeException(ex); 
		}
    }
    
    public static <A extends Appendable> A appendHexArray(A target, char left, byte[] b, int offset, int mask, char right, int bLength) {
	      try {
	    	if (b != null) {        
	            int iMax = bLength - 1;
	            if (iMax == -1) {
	                target.append(left).append(right);
	                return target;
	            }
	            target.append(left);
	            for (int i = 0; ; i++) {
	            	
	            	appendFixedHexDigits(target, 0xFF&b[mask & (i+offset) ], 8);
	                //appendValue(target,b[mask & (i+offset) ]);
	                //target.append(Integer.toString(a[i]));
	                if (i == iMax)
	                    return (A) target.append(right);
	                target.append(", ");
	            } 
	        } else {
	            target.append("null");
	        
	        }
	        return target;
	    } catch (IOException ex) {
			throw new RuntimeException(ex); 
		}
    }
    
    

    public static <A extends Appendable> A appendArray(A target, char left, int[] b, long offset, int mask, char right, int bLength) {
	      try {
	    	if (b != null) {        
	            int iMax = bLength - 1;
	            if (iMax == -1) {
	                target.append(left).append(right);
	                return target;
	            }
	            target.append(left);
	            for (int i = 0; ; i++) {
	                appendValue(target,b[mask & (int)(i+offset) ]);
	                //target.append(Integer.toString(a[i]));
	                if (i == iMax)
	                    return (A) target.append(right);
	                target.append(", ");
	            } 
	        } else {
	            target.append("null");
	        
	        }
	        return target;
	    } catch (IOException ex) {
			throw new RuntimeException(ex); 
		}
   }
   
    public static <A extends Appendable> A appendHexArray(A target, char left, int[] b, long offset, int mask, char right, int bLength) {
	      try {
	    	if (b != null) {        
	            int iMax = bLength - 1;
	            if (iMax == -1) {
	                target.append(left).append(right);
	                return target;
	            }
	            target.append(left);
	            for (int i = 0; ; i++) {
	            	appendFixedHexDigits(target, 0xFFFFFFFF&b[mask & (int)(i+offset) ], 32);

	                if (i == iMax)
	                    return (A) target.append(right);
	                target.append(", ");
	            } 
	        } else {
	            target.append("null");
	        
	        }
	        return target;
	    } catch (IOException ex) {
			throw new RuntimeException(ex); 
		}
 }
    
    public static <A extends Appendable> A appendArray(A target, char left, int[] a, char right) {
	     try {
	    	if (a != null) {        
	            int iMax = a.length - 1;
	            if (iMax == -1) {
	                target.append(left).append(right);
	                return target;
	            }
	            target.append(left);
	            for (int i = 0; ; i++) {
	                appendValue(target,a[i]);
	                //target.append(Integer.toString(a[i]));
	                if (i == iMax)
	                    return (A) target.append(right);
	                target.append(", ");
	            } 
	        } else {
	            target.append("null");
	        
	        }
	        return target;
	    } catch (IOException ex) {
			throw new RuntimeException(ex); 
		}
    }
    
    public static <A extends Appendable> A appendArray(A target, char left, Object[] a, char right) {
	     try{
	    	if (a != null) {        
	            int iMax = a.length - 1;
	            if (iMax == -1) {
	                target.append(left).append(right);
	                return target;
	            }
	            target.append(left);
	            for (int i = 0; ; i++) {
	                target.append((a[i]).toString());
	                if (i == iMax)
	                    return (A) target.append(right);
	                target.append(", ");
	            } 
	        } else {
	            target.append("null");
	        
	        }
	        return target;
	    } catch (IOException ex) {
			throw new RuntimeException(ex); 
		}
    }
    
    public static <A extends Appendable> A appendValue(A target, CharSequence label, int value, CharSequence suffix) {
    	try {
	        appendValue(target,label, value);
	        target.append(suffix);
	        return target;
    	} catch (IOException ex) {
			throw new RuntimeException(ex); 
		}
    }
    
    
    public static <A extends Appendable> A appendValue(A target, CharSequence label, int value) {
    	try {
	        target.append(label);
	        return appendValue(target,value);
    	} catch (IOException ex) {
			throw new RuntimeException(ex); 
		}
    }

    static final int digits = 18;    		
    static final long tens = 1_000_000_000_000_000_000L;
    
    
    static final int digits_small = 6;   
    static final long tens_small = 1_000_000L;
    static final long tens_small_limit = tens_small*10;
    
    
    static final int digits_micro = 2;   
    static final long tens_micro = 100L;
    static final long tens_micro_limit = tens_micro*10;
    

    public static <A extends Appendable> A appendDecimalValue(A target, long m, byte e) {
    	
    	
    	long value = m;    	
    	int g = -e;
    	boolean useParensForNeg = false;
    	
    	try {    		
	        
	        boolean isNegative = value<0;
	        if (isNegative) {
	        	if (useParensForNeg) {
	        		target.append("(-");
	        	} else {
	        		target.append("-");
	        	}
	        	
	            value = -value;
	        }
	        
	        long nextValue = value;
	        int orAll = 0; //this is to remove the leading zeros
	        
	        long temp = Math.abs(value);
	        if (temp<tens_micro_limit) {
	        	decimalValueCollecting(target, digits_micro, tens_micro, g, nextValue, orAll);	        	
	        } else {
		        if (temp<tens_small_limit) {		        	
		        	decimalValueCollecting(target, digits_small, tens_small, g, nextValue, orAll);		        	
		        } else {
		        	decimalValueCollecting(target, digits, tens, g, nextValue, orAll);
		        }
	        }
	        
	        int f = e;
	        f = appendZeros(target, f);	    
	        
	        if (isNegative && useParensForNeg) {
	        	target.append(')');
	        }
	        
	        return target;
	        
	        
    	} catch (IOException ex) {
			throw new RuntimeException(ex); 
		}
    }

	private static <A extends Appendable> int appendZeros(A target, int f) throws IOException {
		while (f>0) {
			target.append('0');
			f--;
		}
		return f;
	}

	
	private static char[]dv = new char[] {'0','1','2','3','4','5','6','7','8','9'};
	
	
	private static <A extends Appendable> void decimalValueCollecting(A target, int digits, long tens, int g,
																		long nextValue, int orAll) throws IOException {
		while (tens>1) {
			
		    int digit = (int)(nextValue/tens);
		    nextValue = nextValue%tens;
		    orAll |= digit;
		    if (0!=orAll || digits<g) {
		        target.append(dv[digit]);//(char)('0'+digit));
		    }
		    
		    if (digits == g) {
		    	target.append('.');
		    }
		    
		    tens /= 10;
		    digits--;
		    
		}
		target.append(dv[(int)nextValue]);//(char)('0'+nextValue));
	}
    
    public static <A extends Appendable> A appendValue(A target, int value) {
    	try {
	        int tens = 1000000000;
	        
	        boolean isNegative = value<0;
	        if (isNegative) {
	            //special case which can not be rendered here.
	            if (value==Integer.MIN_VALUE) {
	                return appendValue(target,(long)value);
	            }
	            
	            target.append("(-");
	            value = -value;
	        }
	        
	        int nextValue = value;
	        int orAll = 0; //this is to remove the leading zeros
	        while (tens>1) {
	            int digit = nextValue/tens;
	            orAll |= digit;
	            if (0!=orAll) {
	                target.append((char)('0'+digit));
	            }
	            nextValue = nextValue%tens;
	            tens /= 10;
	        }
	        target.append((char)('0'+nextValue));
	        if (isNegative) {
	            target.append(")");
	        }
	        return target;
    	} catch (IOException ex) {
			throw new RuntimeException(ex); 
		}
    }
    
    public static <A extends Appendable> A appendHexDigits(A target, int value) {
     try{
    	return (A) appendHexDigitsRaw(target.append("0x"), value);
     } catch (IOException ex) {
		throw new RuntimeException(ex); 
     }
    }
    
    public static <A extends Appendable> A appendHexDigitsRaw(A target, int value) {
		try {
		        int bits = 32 - Integer.numberOfLeadingZeros(value);
		        
		        //round up to next group of 4
		        bits = ((bits+3)>>2)<<2;
		        
		        int nextValue = value;
		        int orAll = 0; //this is to remove the leading zeros
		        while (bits>4) {
		            bits -= 4;            
		            int digit = nextValue>>>bits;
		            orAll |= digit;
		            if (0!=orAll) {
		                target.append(hBase[digit]);            
		            }
		            nextValue =  ((1<<bits)-1) & nextValue;
		        }
		        bits -= 4;
		        target.append(hBase[nextValue>>>bits]);
		        
		        return target;
		} catch (IOException ex) {
			throw new RuntimeException(ex); 
		}
    }
    
    public static <A extends Appendable> A appendValue(A target, CharSequence label, long value, CharSequence suffix) {
	    try {	
	        appendValue(target,label, value);
	        target.append(suffix);
	        return target;
	    } catch (IOException ex) {
			throw new RuntimeException(ex); 
		}
    }
    
    
    public static <A extends Appendable> A appendValue(A target, CharSequence label, long value) {
    	try {
    		target.append(label);
    		return appendValue(target,value);
    	} catch (IOException ex) {
			throw new RuntimeException(ex); 
		}
    }
    
    public static int appendedLength(long value) {
    	int result = value<0?3:0;
    	value = Math.abs(value);
    	
    	while (value > 0) {
    		result++;
    		value = value/10;
    	}
    	
    	return result;
    }
    
    public static <A extends Appendable> A appendValue(A target, long value) {
    	try {
	        long tens = 1000000000000000000L;
	        
	        boolean isNegative = value<0;
	        if (isNegative) {
	            target.append("(-");
	            value = -value;
	        }
	        
	        long nextValue = value;
	        int orAll = 0; //this is to remove the leading zeros
	        while (tens>1) {
	            int digit = (int)(nextValue/tens);
	            orAll |= digit;
	            if (0!=orAll) {
	                target.append((char)('0'+digit));
	            }
	            nextValue = nextValue%tens;
	            tens /= 10;
	        }
	        target.append((char)('0'+nextValue));
	        if (isNegative) {
	            target.append(')');
	        }
	        return target;
    	} catch (IOException ex) {
			throw new RuntimeException(ex); 
		}
    }
    
    public static <A extends Appendable> A appendHexDigits(A target, long value) {
        try{
        	return (A) appendHexDigitsRaw(target.append("0x"), value);
        } catch (IOException ex) {
        	throw new RuntimeException(ex); 
        }
     }
    
    public static <A extends Appendable> A appendHexDigitsRaw(A target, long value) {
        try {
	        int bits = 64-Long.numberOfLeadingZeros(value);
	        
	        //round up to next group of 4
	        bits = ((bits+3)>>2)<<2;
	
	        long nextValue = value;
	        int orAll = 0; //this is to remove the leading zeros
	        while (bits>4) {
	            bits -= 4;            
	            int digit = (int)(0xF&(nextValue>>>bits));
	            orAll |= digit;
	            if (0!=orAll) {
	                target.append(hBase[digit]);            
	            }
	            nextValue =  ((1L<<bits)-1L) & nextValue;
	        }
	        bits -= 4;
	        target.append(hBase[(int)(nextValue>>>bits)]);
	        
	        return target;
    	} catch (IOException ex) {
    		throw new RuntimeException(ex); 
    	}
    }
    
    public static <A extends Appendable> A appendFixedHexDigits(A target, long value, int bits) {
    	
    	value = value & ((1L<<bits)-1L);//we want only the lowest bits
    	
    	try {
	        //round up to next group of 4
	        bits = ((bits+3)>>2)<<2;
	        
	        target.append("0x");
	        long nextValue = value;
	        while (bits>4) {            
	            bits -= 4;
	            target.append(hBase[(int)(0xF&(nextValue>>>bits))]);            
	            nextValue =  ((1L<<bits)-1L) & nextValue;
	        }
	        bits -= 4;
	        target.append(hBase[(int)(0xF&(nextValue>>>bits))]);
	        
	        return target;
    	} catch (IOException ex) {
    		throw new RuntimeException(ex); 
    	}
    }
    
    /*
     * 
     * In order to render a number like 42 with exactly 2 places the tests argument must be set to 10, likewise 042 would require 100
     */
    public static <A extends Appendable> A appendFixedDecimalDigits(A target, int value, int tens) {

    	try {
	        if (value<0) {
	            target.append('-');
	            value = -value;
	        }
	        
	        int nextValue = value;
	        while (tens>1) {
	            target.append((char)('0'+(nextValue/tens)));
	            nextValue = nextValue%tens;
	            tens /= 10;
	        }
	        target.append((char)('0'+nextValue));
	        
	        return target;
    	} catch (IOException ex) {
    		
    		throw new RuntimeException(ex); 
    	}
    }
    
    public static <A extends Appendable> A appendFixedDecimalDigits(A target, long value, int tens) {

    	try {
	        if (value<0) {
	            target.append('-');
	            value = -value;
	        }
	        
	        long nextValue = value;
	        while (tens>1) {
	            target.append((char)('0'+(nextValue/tens)));
	            nextValue = nextValue%tens;
	            tens /= 10;
	        }
	        target.append((char)('0'+nextValue));
	        
	        return target;
    	} catch (IOException ex) {
    		
    		throw new RuntimeException(ex); 
    	}
    }
    
    /*
     * 
     * In order to render an 8 bit number the bits must be set to 8. note that bits can only be in units of 4.
     */
    public static <A extends Appendable> A appendFixedHexDigits(A target, int value, int bits) {

    	//value = value & ((1<<bits)-1);//we want only the lowest bits
    	
    	try {
	        //round up to next group of 4
	        bits = ((bits+3)>>2)<<2;
	        
	        target.append("0x");
	        int nextValue = value;
	        while (bits>4) {            
	            bits -= 4;
	            target.append(hBase[nextValue>>>bits]);            
	            nextValue =  ((1<<bits)-1) & nextValue;
	        }
	        bits -= 4;
	        target.append(hBase[nextValue>>>bits]);
	        
	        return target;
		} catch (IOException ex) {
			throw new RuntimeException(ex); 
		}
    }

    
    public static <A extends Appendable> A appendClass(A target, Class clazz, Class clazzParam) {
    	try {
    		return (A) target.append(clazz.getSimpleName()).append('<').append(clazzParam.getSimpleName()).append("> ");
    	} catch (IOException ex) {
			throw new RuntimeException(ex); 
		}
    }
    
    public static <A extends Appendable> A appendStaticCall(A target, Class clazz, String method) {
    	try {
    		return (A) target.append(clazz.getSimpleName()).append('.').append(method).append('(');
    	} catch (IOException ex) {
			throw new RuntimeException(ex); 
		}
    }
    
    public static StringBuilder truncate(StringBuilder builder) {
        builder.setLength(0);
        return builder;
    }

    //copy the sequence but skip every instance of the provided skip chars
    public static <A extends Appendable> A appendAndSkip(A target, CharSequence source, CharSequence skip) {
        return appendAndSkipImpl(target, source, skip, skip.length(), source.length(), 0, 0);    
    }

    private static <A extends Appendable> A appendAndSkipImpl(A target, CharSequence source, CharSequence skip, int skipLen, int sourceLen, int j, int i) {

	        for(; i<sourceLen; i++) {            
	            if (source.charAt(i)!=skip.charAt(j)) {
	                copyChars(target, source, j, 0, i-j);
	                j=0;
	            } else {
	                if (skipLen == ++j) {
	                    j=0;
	                }
	            }        
	        }
	        return target;
    }

    private static void copyChars(Appendable target, CharSequence source, int j, int k, int base) {
    	try {
	        for(; k<=j ; k++) {                    
	            target.append(source.charAt(base+k));
	        }
    	} catch (IOException ex) {
			throw new RuntimeException(ex); 
		}
    }

    
    private final static char[] base64 = new char[]{'A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P',
    		                                        'Q','R','S','T','U','V','W','X','Y','Z','a','b','c','d','e','f',
    		                                        'g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v',
    		                                        'w','x','y','z','0','1','2','3','4','5','6','7','8','9','+','/'};
    
    // + IS %2B
    // / IS %2F
    // = IS %3D    
    
    
    //TODO: add unit tests for each of the cases listed here https://en.wikipedia.org/wiki/Base64
    
    /**
     * Writes URL encoded base64 encoded value of range found in backing array.
     * 
     */
    public static <A extends Appendable> A appendBase64Encoded(A target, byte[] backing, int pos, int len, int mask) {
        //  https://en.wikipedia.org/wiki/Base64
    	try {
	    	int accumulator = 0;
	    	int i = 0;
	    	int shift = -6;
	    	int count = 0;
	    	while (i < len) {
	    		
	    		shift+=8; // 2 4 (we now have 10)
	    		accumulator = (accumulator<<8) | (0xFF&backing[mask & pos++]);
	    		i++;
	    
				while (shift >= 0) {
					int index = 0x3F&(accumulator>>shift);
					
					if (index<62) {
						target.append(base64[index]);
					} else {
						if (index==62) {
							assert(base64[index]=='+');
							target.append("%2B");
						} else {
							assert(base64[index]=='/');
							target.append("%2F");
						}
					}
		
					shift -= 6; //took top 6 now shift is at -4, 
					count++;
				}   		
	    	}
	    	
	    	if (shift<0) {//last letter.
	    		
	    		shift+=8; 
	    		accumulator = (accumulator<<8) | (0xFF&0);
	    		i++;
	    		
	    		while (shift > 0) {    			
	    			int index = 0x3F&(accumulator>>shift);
					
					if (index<62) {
						target.append(base64[index]);
					} else {
						if (index==62) {
							assert(base64[index]=='+');
							target.append("%2B");
						} else {
							assert(base64[index]=='/');
							target.append("%2F");
						}
					}
	    			shift -= 6; //took top 6 now shift is at -4,
	    			count++;
	    		} 
	    		
	    	}
	    	//NOTE: could and should be optimized.
	        while ((count & 0x03) != 0) {
	        	target.append("%3D");
	        	count++;
	        }
	    
	    	return target;
    	} catch (IOException ioex) {
    		throw new RuntimeException(ioex);
    	}
    }
    
    public static <A extends Appendable> A appendBase64(A target, byte[] backing, int pos, int len, int mask) {
        //  https://en.wikipedia.org/wiki/Base64
    	try {
	    	int accumulator = 0;
	    	int i = 0;
	    	int shift = -6;
	    	int count = 0;
	    	while (i < len) {
	    		
	    		shift+=8; // 2 4 (we now have 10)
	    		accumulator = (accumulator<<8) | (0xFF&backing[mask & pos++]);
	    		i++;
	    
				while (shift >= 0) {
					target.append(base64[0x3F&(accumulator>>shift)]);
					shift -= 6; //took top 6 now shift is at -4, 
					count++;
				}   		
	    	}
	    	
	    	if (shift<0) {//last letter.
	    		
	    		shift+=8; 
	    		accumulator = (accumulator<<8) | (0xFF&0);
	    		i++;
	    		
	    		while (shift > 0) {    			
	    			target.append(base64[0x3F&(accumulator>>shift)]);
	    			shift -= 6; //took top 6 now shift is at -4,
	    			count++;
	    		} 
	    		
	    	}
	    	//NOTE: could and should be optimized.
	        while ((count & 0x03) != 0) {
	        	target.append('=');
	        	count++;
	        }
	    
	    	return target;
    	} catch (IOException ioex) {
    		throw new RuntimeException(ioex);
    	}
    }
    
    public static <A extends Appendable> A appendUTF8(A target, byte[] backing, int pos, int len, int mask) {
    	try {
	        //TODO: note with very long len plus pos we can still run into a problem. so assert len< maxInt-mask ???
	        long localPos = mask&pos;//to support streams longer than 32 bits
	        long charAndPos = ((long)localPos)<<32;
	        long limit = ((long)localPos+len)<<32;
	
	        while (charAndPos<limit) {
	            charAndPos = Pipe.decodeUTF8Fast(backing, charAndPos, mask);
	            target.append((char)charAndPos);
	        }
	        return target;
    	} catch (IOException ex) {
			throw new RuntimeException(ex); 
		}
    }
   
    
	public static CharSequence[] split(CharSequence text, char c) {
		return split(0,0,0,text,c);
	}
	
	private static CharSequence[] split(int pos, int start, int depth, final CharSequence text, final char c) {
		CharSequence[] result;
		while (pos<text.length()) {
			if (text.charAt(pos++)==c) {
				result = split(pos, pos ,depth+1, text, c);
				result[depth] = text.subSequence(start, pos-1);
				return result;
			}
		}
		result = new CharSequence[depth+1];
		result[depth] = text.subSequence(start, text.length());		
		return result;
	}
	
	public static AppendableProxy proxy(Appendable a) {
		return new AppendableProxy(a);
	}
	
	public static AppendableProxy wrap(Appendable a) {
		return new AppendableProxy(a);
	}

	public static <A extends Appendable> A  appendEpochTime(A target, long msTime) {
		 try {
			 appendFixedDecimalDigits(
			 appendFixedDecimalDigits(
			 appendFixedDecimalDigits(
					 appendValue(target, (msTime/(60L*60_000L))).append(':')
					  ,(msTime/60_000L)%60L,10).append(':')
			          ,(msTime/1000L)%60L,10).append('.')
			          ,msTime%1000L,100);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		 
		 
		 return target;
	}


	public static void appendNearestTimeUnit(Appendable target, long nsValue) {
		try {
			if (nsValue<2_000) {
				appendValue(target, nsValue).append("ns");
			} else if (nsValue<2_000_000){
				appendValue(target, nsValue/1_000).append("µs");
			} else if (nsValue<2_000_000_000){
				appendValue(target, nsValue/1_000_000).append("ms");				
			} else if (nsValue<90_000_000_000L){
				appendValue(target, nsValue/1_000_000_000L).append("sec");
			} else if (nsValue<(120L*60_000_000_000L)){
				appendValue(target, nsValue/60_000_000_000L).append("min");
			} else {
				appendValue(target, nsValue/(60L*60_000_000_000L)).append("hr");
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	//TODO: add nearestMemoryUnit  B, K, M, G, T, P
    
}
