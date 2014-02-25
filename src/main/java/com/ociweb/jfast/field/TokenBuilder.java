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
	 * 18 instance - max fields 262144 (or pmax bytes, or destination)
	 * 
	 * read the type first then each can have its own operation parse logic.
	 * this is used by decimal in order to get two operations 
	 * groups and dictionarys have their own operators as well
	 *  
	 *
	 * groups often use multiple tokens in a row from the script to satify the 
	 * required arguments.
	 * group type operators - Open/Close, Repeats(second int)
	 * read type operators  - Read from (into next field in use)  
	 *
	 *
	 */
	
	public static final int MAX_INSTANCE       = 0x3FFFF; //2^18 max fields 262144
	//See fast writer for details and mask sizes
	public static final int MASK_TYPE          = 0x1F; //5 bits
	
	public static final int MAX_FIELD_ID_BITS  = 18;
	public static final int MAX_FIELD_ID_VALUE = (1<<MAX_FIELD_ID_BITS)-1;
	public static final int MAX_FIELD_MASK     = 0xFFFFFFFF^MAX_FIELD_ID_VALUE;
	
	public static final int SHIFT_DICT         = MAX_FIELD_ID_BITS;
	public static final int BITS_DICT          = 2;
	public static final int SHIFT_OPER         = SHIFT_DICT+BITS_DICT;
	public static final int BITS_OPER          = 6;
	public static final int SHIFT_TYPE         = SHIFT_OPER+BITS_OPER;
	public static final int BITS_TYPE          = 5;
	
	public static final int MASK_OPER          = 0x3F; //6 bits
	public static final int MASK_OPER_DECIMAL  = 0x07; //3 bits
	public static final int SHIFT_OPER_DECIMAL = 3; 
	
	
	//sequence is stored as a length field type which appears in the stream before the repeating children.
	//an optional sequence will use the optional length as its encoding to be absent.
	//template ref must appear before length because it can modify length operator.

	public static int extractType(int token) {
		return SHIFT_TYPE&(token>>SHIFT_TYPE);
	}
	public static int extractCount(int token) {
		return MAX_INSTANCE&token;
	}

	
	public static boolean isOptional(int token) {
		return (0!=(token&(1<<TokenBuilder.SHIFT_TYPE)));
	}
	
	public static int buildToken(int tokenType, int tokenOpp, int count) {
		assert(count<=MAX_INSTANCE);
		if (tokenType==TypeMask.Decimal || tokenType==TypeMask.DecimalOptional) {
			if (tokenOpp>TokenBuilder.MASK_OPER_DECIMAL) {
				throw new UnsupportedOperationException("operator not supported by decimal.");
			}
			
			//simple build for decimal, may want another method to do two types
			return 0x80000000 |  
				       (tokenType<<TokenBuilder.SHIFT_TYPE) |
				       (tokenOpp<<(TokenBuilder.SHIFT_OPER+TokenBuilder.SHIFT_OPER_DECIMAL)) |
				       (tokenOpp<<TokenBuilder.SHIFT_OPER) |
				       count&MAX_INSTANCE;
			
		} else {
			return 0x80000000 |  
			       (tokenType<<TokenBuilder.SHIFT_TYPE) |
			       (tokenOpp<<TokenBuilder.SHIFT_OPER) |
			       count&MAX_INSTANCE;
		}
	}
	
	public static boolean isInValidCombo(int type, int operator) {
		if (type>=0 && type<=TypeMask.LongSignedOptional) {
			//integer/long types do not support tail
			if (OperatorMask.Field_Tail==operator) {
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
			return ("token: "+TypeMask.toString(type)+" "+OperatorMask.toString(type, opp1)+" "+OperatorMask.toString(type, opp2)+" "+count);
			
		} else {
			int opp  = (token>>TokenBuilder.SHIFT_OPER)&TokenBuilder.MASK_OPER;
			if (isInValidCombo(type,opp)) {
				throw new UnsupportedOperationException("bad token");
			};
			
			return ("token: "+TypeMask.toString(type)+" "+OperatorMask.toString(type, opp)+" "+count);
		}
		
	}




	
}
