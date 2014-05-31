package com.ociweb.jfast.benchmark;

import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.field.TypeMask;

public class TestUtil {

    public static int[] buildTokens(int count, int[] types, int[] operators) {
    	int[] lookup = new int[count];
    	int typeIdx = types.length-1;
    	int opsIdx = operators.length-1;
    	while (--count>=0) {
    		//high bit set
    		//  7 bit type (must match method)
    		//  4 bit operation (must match method)
    		// 20 bit instance (MUST be lowest for easy mask and frequent use)
    
    		//find next pattern to be used, rotating over them all.
    		do {
    			if (--typeIdx<0) {
    				if (--opsIdx<0) {
    					opsIdx = operators.length-1;
    				}
    				typeIdx = types.length-1;
    			}
    		} while (TestUtil.isInValidCombo(types[typeIdx],operators[opsIdx]));
    		
    		int tokenType = types[typeIdx];
    		int tokenOpp = operators[opsIdx];
    		
    		//When testing decimals the same operator is used for both exponent and mantissa.
    		if (tokenType == TypeMask.Decimal || 
    			tokenType == TypeMask.DecimalOptional) {
    			
    			tokenOpp |= tokenOpp<<TokenBuilder.SHIFT_OPER_DECIMAL_EX;
    			
    		}
    		
    		lookup[count] = TokenBuilder.buildToken(tokenType, tokenOpp, count, TokenBuilder.MASK_ABSENT_DEFAULT);
    				
    	}
    	return lookup;
    	
    }

    public static boolean isInValidCombo(int type, int operator) {
    	
    	return OperatorMask.Field_Tail==operator && type<=TypeMask.LongSignedOptional;
    	
    }

}
