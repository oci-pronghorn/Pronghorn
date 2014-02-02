package com.ociweb.jfast.loader;

public class CatalogFactory {

	private static final int INIT_GROW_STEP = 16;
	
	int[] idArray = new int[INIT_GROW_STEP];
	int[] tokenArray = new int[INIT_GROW_STEP];
	int idCount;
	
	public CatalogFactory() {
		
	}
	
	public void addId(int id, int token) {
		idArray[idCount] = id;
		tokenArray[idCount] = token;
		if (++idCount>=idArray.length) {
			int newLength = idCount+INIT_GROW_STEP;
			int[] temp1 = new int[newLength];
			int[] temp2 = new int[newLength];
			System.arraycopy(idArray, 0, temp1, 0, idArray.length);
			System.arraycopy(tokenArray, 0, temp1, 0, tokenArray.length);
			idArray = temp1;
			tokenArray = temp2;
		}
	}
	
	
}
