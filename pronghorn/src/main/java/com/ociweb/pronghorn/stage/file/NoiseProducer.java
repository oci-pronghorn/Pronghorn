package com.ociweb.pronghorn.stage.file;

import java.security.SecureRandom;

import com.ociweb.pronghorn.pipe.ChannelWriter;

public class NoiseProducer {

	private final SecureRandom sr;
	
	public NoiseProducer(SecureRandom sr) {
		this.sr = sr;
	}
	
	//NOTE: also writes leading backed int for size
	public void writeNoise(ChannelWriter target, long id, int maxNoise) {
		int size = sr.nextInt(maxNoise); 
		
		if (size<=0) {
			target.writePackedInt(0);
		} else {
					
			target.writePackedInt(size);
			
			int bytesRemaining = size&0x3;
			int intsRemaining = size>>2;
	
			while(--bytesRemaining>=0) {
				target.writeByte(sr.nextInt(256));
			}
			
			while (--intsRemaining>=0) {
				target.writeInt(sr.nextInt());
			}
		}
		
		
	}

	public byte[] nextCypherBlock() {
		byte[] cypherBlock = new byte[16];
		sr.nextBytes(cypherBlock);	
		return cypherBlock;
	}
	
}

