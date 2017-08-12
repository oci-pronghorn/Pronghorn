package com.ociweb.pronghorn.stage.encrypt;

import static com.ociweb.pronghorn.pipe.Pipe.blobMask;
import static com.ociweb.pronghorn.pipe.Pipe.byteBackingArray;
import static com.ociweb.pronghorn.pipe.Pipe.bytePosition;

import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.pipe.token.LOCUtil;
import com.ociweb.pronghorn.pipe.token.TypeMask;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class RawDataCryptAESCBCPKCS5Stage extends PronghornStage {

	private byte[] iv;
	private Cipher c;
	
	private final byte[] pass;
	private final Pipe<RawDataSchema> input;
	private final Pipe<RawDataSchema> output;
		
	private int length;
	private int position;
	private byte[] backing;
	private int mask;
	
	private final boolean encrypt;
	
	private final int passSizeBits = 4;
	private final int passSize = 1<<passSizeBits;
	private final int passSizeMask = passSize-1;
	
	/////////////////////////////////////////////////////////////
	//Please read these for a better understanding of AES-CBC
	/////////////////////////////////////////////////////////////
	//https://en.wikipedia.org/wiki/Advanced_Encryption_Standard
	//https://en.wikipedia.org/wiki/Block_cipher_mode_of_operation#Cipher_Block_Chaining_.28CBC.29
	/////////////////////////////////////////////////////////////
	
	public RawDataCryptAESCBCPKCS5Stage(GraphManager graphManager, 
				                         byte[] pass,
				                         boolean encrypt,
				                         Pipe<RawDataSchema> input, 
				                         Pipe<RawDataSchema> output) {
		super(graphManager, input, output);
		if (pass.length!=passSize) {
			throw new UnsupportedOperationException("pass must be "+passSize+" bytes");
		}
		this.encrypt = encrypt;
		this.pass = pass;
		this.input = input;
		this.output = output;
	}

	@Override
	public void startup() {
		
		try {
			
			c = Cipher.getInstance("AES/CBC/PKCS5Padding");
			iv = new byte[passSize];

			c.init(encrypt ? Cipher.ENCRYPT_MODE : Cipher.DECRYPT_MODE, 
				   new SecretKeySpec(pass, "AES"),
				   new IvParameterSpec(iv));

			
			//only need this block if we want to read iv
			//AlgorithmParameters params = c.getParameters();
			//params.getParameterSpec(paramSpec)
			//ivBytes = params.getParameterSpec(IvParameterSpec.class).getIV()
			
		} catch (NoSuchAlgorithmException | NoSuchPaddingException e) {
			throw new RuntimeException(e);
		} catch (InvalidKeyException e) {
			throw new RuntimeException(e);
		} catch (InvalidAlgorithmParameterException e) {
			throw new RuntimeException(e);
		}
		
	}

	
	@Override
	public void run() {
		
		while (length>0 && processAvail()) {}
		
		while (0 == length && Pipe.hasContentToRead(input)) {
			
		    int msgIdx = Pipe.takeMsgIdx(input);
		    switch(msgIdx) {
		        case RawDataSchema.MSG_CHUNKEDSTREAM_1:
		           newBlock(input);
		           Pipe.confirmLowLevelRead(input, Pipe.sizeOf(RawDataSchema.instance, RawDataSchema.MSG_CHUNKEDSTREAM_1));
		        break;
		        case -1:
		           Pipe.spinBlockForRoom(output, Pipe.EOF_SIZE);	
		           Pipe.publishEOF(output);	
		           Pipe.confirmLowLevelRead(input, Pipe.EOF_SIZE);	
		           requestShutdown();
		        break;
		    }
		    Pipe.releaseReadLock(input);		    
		}
	}

	
	private void newBlock(Pipe<RawDataSchema> input) {
		
		int mta = Pipe.takeRingByteMetaData(input);
		length = Pipe.takeRingByteLen(input);
		mask = blobMask(input);	
		position = bytePosition(mta, input, length)&mask;     		
		backing = byteBackingArray(mta, input);

		while (length>0 && processAvail()){}
	
	}

	private boolean processAvail() {
	
		int distSourceToWrap = mask-(position&mask);
				
		final int targetPos = Pipe.getWorkingBlobHeadPosition(output);
		final byte[] targetBuffer = Pipe.blob(output);
		final int byteMask = Pipe.blobMask(output);
		
		int distTargetToWrap =  byteMask-(targetPos&byteMask);
		
		int availSourceLen = Math.min(length, distSourceToWrap);
		
		//compute target size, round up to next passSize (block size)
		int targetSize = ((availSourceLen+passSizeMask)>>passSizeBits)<<passSizeBits;
		
		int availTargetSize = Math.min(targetSize, Math.min(output.maxVarLen, distTargetToWrap ));
		
		if (availTargetSize < targetSize) {
			//must lower source since the target got limited.
			availSourceLen = availTargetSize-passSize;
		}
		//System.err.println("target size "+targetSize+" source len "+availSourceLen+"  "+encrypt);
		
		//For most common case this conditional will be true
		if (Pipe.hasRoomForWrite(output) ) {		
			int size= Pipe.addMsgIdx(output, RawDataSchema.MSG_CHUNKEDSTREAM_1);
			
			try {
				
				int encSize = c.doFinal(
										  backing, 
										  position, 
										  availSourceLen, 
										  targetBuffer, 
										  targetPos);
				int loc = RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2;
					
				
				assert(LOCUtil.isLocOfAnyType(loc, TypeMask.TextASCII, TypeMask.TextASCIIOptional, TypeMask.TextUTF8, TypeMask.TextUTF8Optional, TypeMask.ByteVector, TypeMask.ByteVectorOptional)): "Value found "+LOCUtil.typeAsString(loc);
				
				
				Pipe.addBytePosAndLen(output, targetPos, encSize);
				Pipe.addAndGetBytesWorkingHeadPosition(output, encSize);
								
				position += availSourceLen;
				length -= availSourceLen;
				
			} catch (ShortBufferException e) {
				throw new RuntimeException(encrypt?"failure while encrypting":"failure while decrypting",e);
			} catch (IllegalBlockSizeException e) {
				throw new RuntimeException(encrypt?"failure while encrypting":"failure while decrypting",e);
			} catch (BadPaddingException e) {
				throw new RuntimeException(encrypt?"failure while encrypting":"failure while decrypting",e);
			}
						
			Pipe.confirmLowLevelWrite(output, size);
			Pipe.publishWrites(output);
						
			return true;
		} else {
			return false;//no room to do work
		}
	}

	
}
