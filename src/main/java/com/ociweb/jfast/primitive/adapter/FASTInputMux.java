package com.ociweb.jfast.primitive.adapter;

import com.ociweb.jfast.primitive.FASTInput;



public class FASTInputMux implements FASTInput {

	final FASTInput sourceA;
	final FASTInput sourceB;
	final FASTInput sourceC;
	
	int headA, tailA;
	int headB, tailB;
	int headC, tailC;
	long posA, posB, posC;
	
	byte[] targetBuffer;
	byte[] working;
		
	long sendingPos;

	final int FETCH_SIZE = 256;
	final int FETCH_MASK = FETCH_SIZE-1;
	
	public FASTInputMux(FASTInput sourceA, 
			             FASTInput sourceB, 
			             FASTInput sourceC) {
		this.sourceA = sourceA;
		this.sourceB = sourceB;
		this.sourceC = sourceC;
				
		this.working = new byte[FETCH_SIZE*3];
	}
	
	//TODO: C, return the failure state for external manager (must be another thread!).
	//TODO: C, for fail over, external manager can startup new FASTInput and add it here once its running.
	
	
	
	@Override
	public int fill(int offset, int count) {
		//
		//toss out any old data from slow feeds that are catching up.
		//
		if (posA<sendingPos && headA<tailA) {
			long step = Math.min(sendingPos-posA,tailA-headA);
			headA+=step;
			posA+=step;
			//reset back to beginning because we do not loop
			if (headA==tailA) {
				headA=tailA=0;
			}
		}
		if (posB<sendingPos && headB<tailB) {
			long step = Math.min(sendingPos-posB,tailB-headB);
			headB+=step;
			posB+=step;
			//reset back to beginning because we do not loop
			if (headB==tailB) {
				headB=tailB=0;
			}
		}
		if (posC<sendingPos && headC<tailC) {
			long step = Math.min(sendingPos-posC,tailC-headC);
			headC+=step;
			posC+=step;
			//reset back to beginning because we do not loop
			if (headC==tailC) {
				headC=tailC=0;
			}
		}
		//
		//load available data			
		//
		tailA += sourceA.fill(tailA, Math.min( (FETCH_SIZE - (FETCH_MASK & tailA)), count));
		tailB += sourceB.fill(tailB, Math.min( (FETCH_SIZE - (FETCH_MASK & tailB)), count));
		tailC += sourceC.fill(tailC, Math.min( (FETCH_SIZE - (FETCH_MASK & tailC)), count));
		
		//
		//check for matching values, TODO: Z, multi reduntant feeds. Far from the best implementation but good enough for now.
		//
		int tmp = 0;
		if (posA == sendingPos) {
			if (posB == sendingPos) {
				if (posC == sendingPos) {
					//ready to use A B and C
					while(working[headA] == working[headB] && 
						   working[headB] == working[headC] &&
						   headA<tailA && 
						   headB<tailB && 
						   headC<tailC) {
						
						targetBuffer[offset++] = working[headA];
						headA++;
						headB++;
						headC++;						
						tmp++;
					}
					
					if (working[headA] == working[headB]) {
						//A and B match
						int r = copyCheckedValues(offset, Math.min(tailA-headA, tailB-headB), headA, headB);
						headA += r;
						headB += r;
						tmp+=r;
						
						//sourceC.reset(); //we need to do this but not block on the connection wait.
						//we also need the position after the reset.

					} else {
						if (working[headB] == working[headC]) {
							//B and C match
							int r = copyCheckedValues(offset, Math.min(tailB-headB, tailC-headC), headB, headC);
							headB += r;
							headC += r;
							tmp+=r;
							
						} else {
							if (working[headA] == working[headC]) {
								//A and C match
								int r = copyCheckedValues(offset, Math.min(tailA-headA, tailC-headC), headA, headC);
								headA += r;
								headC += r;
								tmp+=r;
							} 
						}						
					}
				} else {
					//ready to use A and B but NO C
					int r = copyCheckedValues(offset, Math.min(tailA-headA, tailB-headB), headA, headB);
					headA += r;
					headB += r;
					tmp+=r;
				}				
			} else {
				//no B so C is now required or the check can not be made.
				if (posC == sendingPos) {
				    //ready to use A and C but NO B
					int r = copyCheckedValues(offset, Math.min(tailA-headA, tailC-headC), headA, headC);
					headA += r;
					headC += r;
					tmp+=r;
				}
			}
		} else {
			//unable to use A
			if (posB == sendingPos & posC==sendingPos) {
				//ready to use B and C but NO A
				int r = copyCheckedValues(offset, Math.min(tailB-headB, tailC-headC), headB, headC);
				headB += r;
				headC += r;
				tmp+=r;
			} 
		}
		//unable to check anything at this time
		return tmp;
	}

	private int copyCheckedValues(int offset, int len, int i, int j) {
		int c = 0;
		while (--len>0 && working[i]==working[j]) {
			targetBuffer[offset++] = working[i];						
			i++;
			j++;
			c++;
		}
		return c;
	}

	@Override
	public void init(byte[] targetBuffer) {
		this.targetBuffer = targetBuffer;
		this.sourceA.init(working);
		this.sourceB.init(working);
		this.sourceC.init(working);
		
		int offset = 0;
		headA = tailA = offset;
		
		offset+=FETCH_SIZE;
		headB = tailB = offset;
		
		offset+=FETCH_SIZE;
		headC = tailC = offset;
				
	}

	@Override
	public boolean isEOF() {
		return sourceA.isEOF() && sourceB.isEOF() && sourceC.isEOF();
	}

    @Override
    public void block() {
        // TODO Auto-generated method stub
        
    }

}
