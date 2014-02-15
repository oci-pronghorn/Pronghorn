package com.ociweb.jfast.primitive.adapter;

import com.ociweb.jfast.primitive.FASTInput;


/**
 * Reads from all the available inputs and fills the buffer with the "fastest" and
 * "most agreed upon" values from the feed.
 * 
 * "Fastest" and "most agreed upon" is determined by the the first n inputs to
 * agree on the provided the data where n is set on startup.
 * 
 * For example if n is set to 2 and 5 inputs are provided. And further if the first
 * two inputs to respond do not agree and the third does agree with one of the first two 
 * at that time the result will be returned.  The remaining two inputs will have the data
 * loaded to provide a hot fail over but it will not be used.
 *    
 * 
 */
public class FASTInputMux implements FASTInput {

	final FASTInput[] sources;
	final int sourceCount;
	byte[] targetBuffer;
	byte[] working;
	int[] heads;
	int[] tails;
	long[] pos;
	
	long sendingPos;

	final int matches;
	final int FETCH_SIZE = 256;
	final int FETCH_MASK = FETCH_SIZE-1;
	
	public FASTInputMux(FASTInput[] sources, int matches) {
		this.sources=sources;
		this.sourceCount = sources.length;
		this.working = new byte[FETCH_SIZE*sourceCount];
		//these never wrap and are moved back down to 0 when possible
		this.heads = new int[sourceCount];
		this.tails = new int[sourceCount];//exclusive
		this.pos = new long[sourceCount];
		this.matches = matches;
	}
	
	@Override
	public int fill(int offset, int count) {
		
		//int lastByte = Integer.MAX_VALUE;
		//int matchCount = 0;
		
		long checker = 0; //TODO: sources may not be > 8
		
		int i = sources.length;
		while (--i>=0) {
			int maxFill = Math.min( (FETCH_SIZE - (FETCH_MASK&tails[i])), count);
			int result = 0==maxFill ? 0 : sources[i].fill(tails[i], maxFill);
			
			tails[i]+=result;
			//each source is up to date for what is has at the moment.
			//
			//toss out any old data from slow feeds that are catching up.
			if (pos[i]<sendingPos && heads[i]<tails[i]) {
				long step = Math.min(sendingPos-pos[i],tails[i]-heads[i]);
				heads[i]+=step;
				pos[i]+=step;
				//reset back to beginning because we do not loop
				if (heads[i]==tails[i]) {
					heads[i]=tails[i]=0;
				}
			}
			
			// 0 1 2 3 4 5
			//  x   x   x
            //    x   x
			
			
			//xor the even ones, and xor the odd ones. everything should be zero.
			
			
			//now check for matching data
			if (pos[i]==sendingPos && heads[i]<tails[i]) {
				checker = (checker<<8)|working[heads[i]];
				
//				//head has the value we are after
//				byte datum = working[heads[i]];
//				if (datum == lastByte) {
//					matchCount++;					
//				} else {
//					if (matchCount==0) {
//						//switch to this new value if we have not found any matches so far
//						lastByte=datum;						
//					} 
//				}
				
				heads[i]++;
				pos[i]++;
				
				
				
			}
			
			
		}
		
		//TODO: how to deal with this long to know about match count?
		
		//assuming that 1 is always a failure
		//rotate 8bits for each of the inputs-1
		//and xor the results together.
		
		// 0 1 2 3
		// 1 2 3 0
		
		// 0 1 2 3
		// 3 0 1 2 
		
		// 0 1 2 3
		// 2 3 0 1  
				
		//6 zeros which is 3! and we need 3
		
		//0 1 2
		//2 0 1
		
		//0 1 2
		//1 2 0
		
		//2 zeros which is 2! and we need 2
		
		
				
		
		
		return 1;
	}

	@Override
	public void init(byte[] targetBuffer) {
		this.targetBuffer = targetBuffer;
		
		int i = sources.length;
		while (--i>=0) {
			sources[i].init(working);
			int offset = i*FETCH_SIZE;
			heads[i] = offset;
		    tails[i] = offset;
		    //pos is zero for the first byte of stream
		}
		
	}

}
