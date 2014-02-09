//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.field;


public class TokenBuilder {

	/*   32 bits total
	 *******************
	 *  1 token flag
	 *  5 type   (1 new 2x spec types, 3 existing types, 1 isOptional)
	 *  6 operation  (for decimal its 3 and 3) all others use bottom 4
	 *  2 dictionary
	 * 18 instance - max fields 262144
	 * 
	 * read the type first then each can have its own operation parse logic.
	 * this is only used by decimal in order to get two operations 
	 *
	 * groups - eg top group is a message.
	 * pmap bits mask
	 * **************** TOOD: how do I know this is a group an not above call?
	 *   1 token flag
	 *   9 (4bits exp, 5bits mantissa) pmap max bytes
	 *  22 sequence length 4M max 
	 * 
	 * ******* TODO: template script needs copy tokens as well (just add new opp?)
	 * 1 token flat
	 * 5 type
	 * 18 source (following token is the destination)
	 */
	
    //group
	//top 6 bits consumed - 26 bits left for group.
	//max pmap bytes 3&7 10  pmap length
	// 0   1       0  -  127
	// 1   2     127  -  381
	// 2   4     381  -  889
	// 3   8     889  - 1905 
	// 4   16   1905 -  3937
	// 5   32   3937  - 8001
	// 6   64   8001 - 16129
	// 7  127  16129 - 32258
	
	//
	//max seq  16 field id.  
	//sequence is just a normal field found in the intArrray? then we just need the id.
	//it is uint32 and can have all the operators therefore we only need to store its field id.
	
	
	//new binary type, with 9 bits we can
	//hit the significant values up to 1M
	//4 bits of base 2 exponent 2^0 to 2^15 (shift count)
	//5 bits of mantissa 0-32
	
	public static final int MAX_INSTANCE = 0x3FFFF; //2^18 max fields 262144
	//See fast writer for details and mask sizes
	public static final int MASK_TYPE = 0x1F; //5 bits
	public static final int SHIFT_TYPE = 26;  
	
	public static final int MASK_OPER          = 0x3F; //6 bits
	public static final int SHIFT_OPER         = 20;
	public static final int MASK_OPER_DECIMAL  = 0x07; //3 bits
	public static final int SHIFT_OPER_DECIMAL = 3; 
	
			

	public static int buildGroupToken(int typeMask, int maxPMapBytes, int seqFieldId) {
		//must add dynamic/none for template id.
		
		//integer log compression
		int s = 0;
		while (maxPMapBytes>63 && s<15) {
			s++;
			maxPMapBytes>>=1;
		}//4bits of exponent  6bits of value. can go up  to 2mil which would require 21 bits vs our 10 here.	
		if (s>0) {
			//must round up value to ensure we do not trim the pmap.
			maxPMapBytes++;
		}
		return 	0x80000000 | (typeMask<<26) | (0xF&s)<<22 | (0x3F&maxPMapBytes)<<16 | (seqFieldId&0xFFFF);
	}

    public static int extractType(int token) {
		return 0x1F&(token>>26);
	}
	public static int extractMaxBytes(int token) {
		//integer log decompression
		return  (0x3F&(token>>16))<< (0xF&(token>>22));	
	}
	public static int extractSeqLenId(int token) {
		return token&0xFFFF;
	}
	
	public static boolean isOptional(int token) {
		return (0!=(token&(1<<TokenBuilder.SHIFT_TYPE)));
	}
	
	public static int buildToken(int tokenType, int tokenOpp, int count) {
		
		if (tokenType==TypeMask.Decimal || tokenType==TypeMask.DecimalOptional) {
			if (tokenOpp>TokenBuilder.MASK_OPER_DECIMAL) {
				throw new UnsupportedOperationException("operator not supported by decimal.");
			}
			
			//simple build for decimal, may want another method to do two types
			return 0x80000000 |  
				       (tokenType<<TokenBuilder.SHIFT_TYPE) |
				       (tokenOpp<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL)) |
				       (tokenOpp<<TokenBuilder.SHIFT_OPER) |
				       count;
			
		} else {
			return 0x80000000 |  
			       (tokenType<<TokenBuilder.SHIFT_TYPE) |
			       (tokenOpp<<TokenBuilder.SHIFT_OPER) |
			       count;
		}
	}
	
	public static boolean isInValidCombo(int type, int operator) {
		if (type>=0 && type<=TypeMask.LongSignedOptional) {
			//integer/long types do not support tail
			if (OperatorMask.Tail==operator) {
				return true;
			}
		}		
		
		return false;
	}
	
	public static boolean isOpperator(int token, int operator) {
		int type = (token>>TokenBuilder.SHIFT_TYPE)&TokenBuilder.MASK_TYPE;
		
		if (type==TypeMask.Decimal || type==TypeMask.DecimalOptional) {
			return ((token>>TokenBuilder.SHIFT_OPER)&TokenBuilder.MASK_OPER_DECIMAL)==operator||((token>>(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL))&TokenBuilder.MASK_OPER_DECIMAL)==operator;
		} else {
			return ((token>>TokenBuilder.SHIFT_OPER)&TokenBuilder.MASK_OPER)==operator;
		}
	}
	
	public static String tokenToString(int token) {
		
		int type = (token>>TokenBuilder.SHIFT_TYPE)&TokenBuilder.MASK_TYPE;
		int count = token & TokenBuilder.MAX_INSTANCE;
		
		if (type==TypeMask.Decimal || type==TypeMask.DecimalOptional) {
			int opp1 = (token>>(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL))&TokenBuilder.MASK_OPER_DECIMAL;
			int opp2 = (token>>TokenBuilder.SHIFT_OPER)&TokenBuilder.MASK_OPER_DECIMAL;
			if (isInValidCombo(type,opp1)) {
				throw new UnsupportedOperationException("bad token");
			};
			if (isInValidCombo(type,opp2)) {
				throw new UnsupportedOperationException("bad token");
			};
			return ("token: "+TypeMask.toString(type)+" "+OperatorMask.toString(opp1)+" "+OperatorMask.toString(opp2)+" "+count);
			
		} else {
			int opp  = (token>>TokenBuilder.SHIFT_OPER)&TokenBuilder.MASK_OPER;
			if (isInValidCombo(type,opp)) {
				throw new UnsupportedOperationException("bad token");
			};
			
			return ("token: "+TypeMask.toString(type)+" "+OperatorMask.toString(opp)+" "+count);
		}
		
	}




	
}
