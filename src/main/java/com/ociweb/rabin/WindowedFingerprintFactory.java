package com.ociweb.rabin;

import java.math.BigInteger;

import org.rabinfingerprint.polynomial.Polynomial;

//Note this class is NOT garbage free and should only be used when building the new catalog and support files.
public class WindowedFingerprintFactory {

	//TODO: All this must be refactored for use inside the catalog builder.
	
	
	public static WindowedFingerprint buildNew() {
		
		Polynomial p = Polynomial.createIrreducible(53);
		
		//These 4 fields will need to be saved for re-use each time the catalog is loaded
		
		int windowSize;//power of two based on loaded templates and embeded sequences 
		
		int degree;// derived from the selected Poly 
		long[] pushTable;// derived from the selected Poly 
		long[] popTable;// derived from the selected Poly
				
		windowSize = 64;
		popTable = popTable(p,windowSize);
		pushTable = pushTable(p);
		
		degree = p.degree().intValue();
		
		
		return new WindowedFingerprint(windowSize, degree, pushTable, popTable);
	}
	
	private static long[] popTable(Polynomial poly, int bytesPerWindow) {
		int i = 256;
		long[] popTable = new long[i];
		BigInteger shft = BigInteger.valueOf(bytesPerWindow * 8);
		while (--i>=0) {
			popTable[i] = Polynomial.createFromLong(i).shiftLeft(shft).mod(poly).toBigInteger().longValue();
		}
		return popTable;
	}
	
	private static long[] pushTable(Polynomial poly) {
		int i = 512;
		long[] pushTable = new long[i];
		BigInteger degree = poly.degree();
		while (--i>=0) {
			Polynomial f = Polynomial.createFromLong(i).shiftLeft(degree);
			pushTable[i] = f.xor(f.mod(poly)).toBigInteger().longValue();
		}
		return pushTable;
	}
	
}
