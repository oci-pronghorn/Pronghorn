package com.ociweb.jfast.field;


public class TokenBuilder {

	/*   32 bits total
	 *******************
	 *  1 token flag
	 *  5 type   (1 new 2x spec types, 3 existing types, 1 isOptional)
	 *  6 operation  (for decimal its 3 and 3) all others use bottom 4
	 * 20 instance
	 * 
	 *read the type first then each can have its own operation parse logic.
	 * 
	 * pmap bits mask
	 * ****************
	 *   1 token flag
	 *   9 (4bits exp, 5bits mantissa) pmap max bytes
	 *  22 sequence length 4M max 
	 * 
	 */
 
	//new binary type, with 9 bits we can
	//hit the significant values up to 1M
	//4 bits of base 2 exponent 2^0 to 2^15 (shift count)
	//5 bits of mantissa 0-32
	
	public static final int MAX_INSTANCE = 0xFFFFF;
	//See fast writer for details and mask sizes
	public static final int MASK_TYPE = 0x1F; //5 bits
	public static final int SHIFT_TYPE = 26;  
	
	public static final int MASK_OPER          = 0x3F; //6 bits
	public static final int SHIFT_OPER         = 20;
	public static final int MASK_OPER_DECIMAL  = 0x07; //3 bits
	public static final int SHIFT_OPER_DECIMAL = 3; 
	
	
	//group pmap
	public static final int MASK_PMAP_MAX = 0x7FF;
	public static final int SHIFT_PMAP_MASK = 20;
		
	
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
		boolean isOptional = 1==(type&0x01);
		
		if (OperatorMask.Constant==operator & isOptional) {
			//constant operator can never be of type optional
			return true;
		}
		
		if (type>=0 && type<=TypeMask.LongSignedOptional) {
			//integer/long types do not support tail
			if (OperatorMask.Tail==operator) {
				return true;
			}
		}		
		
		return false;
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
