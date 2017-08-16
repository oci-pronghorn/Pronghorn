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

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.pipe.token.LOCUtil;
import com.ociweb.pronghorn.pipe.token.TypeMask;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.Appendables;

public class RawDataCryptAESCBCPKCS5Stage extends PronghornStage {

	private byte[] iv;
	private Cipher c;
	
	private final byte[] pass;
	private final Pipe<RawDataSchema> input;
	private final Pipe<RawDataSchema> output;

	private byte[] sourceBuffer;
	private int sourceMask;
	private int blockSize;
	
	private int targetMask;
	
	private final boolean encrypt;
	
	private final int passSizeBits = 4;
	private final int passSize = 1<<passSizeBits;
	private final int passSizeMask = passSize-1;
	
	private DataInputBlobReader<RawDataSchema> inputStream;
	
	private byte[] targetBuffer;
	
	private final int fieldLOC = RawDataSchema.MSG_CHUNKEDSTREAM_1_FIELD_BYTEARRAY_2;
	
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
		
		this.targetBuffer = Pipe.blob(output);
		this.targetMask = Pipe.blobMask(output);
		this.sourceBuffer = Pipe.blob(input);
		this.sourceMask = Pipe.blobMask(input);
		this.inputStream = Pipe.inputStream(input);
		
		try {
			
			c = Cipher.getInstance("AES/CBC/PKCS5Padding");
			iv = new byte[passSize];

			c.init(encrypt ? Cipher.ENCRYPT_MODE : Cipher.DECRYPT_MODE, 
				   new SecretKeySpec(pass, "AES"),
				   new IvParameterSpec(iv));

			blockSize = c.getBlockSize();
			
			assert(blockSize <= input.sizeOfBlobRing) : "block size must be smaller or equal to blob size";
			assert(0 == (input.sizeOfBlobRing%blockSize)) : "block size must fit into blob evently";
			
			
			
			
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

	private boolean pendingShutdown = false;
	
	@Override
	public void run() {
		
		if (pendingShutdown) {
			//this pattern eliminates need for spin-lock which can cause unit test to hang.
			if (Pipe.hasRoomForWrite(output)) {
				 Pipe.publishEOF(output);
				 requestShutdown();
			}
			return;
		}

		while (processAvail()) {}
		
		while (Pipe.hasContentToRead(input)) {

		    int msgIdx = Pipe.takeMsgIdx(input);
		    switch(msgIdx) {
		        case RawDataSchema.MSG_CHUNKEDSTREAM_1:
		        	
		           int len = inputStream.accumLowLevelAPIField();
		           Pipe.confirmLowLevelRead(input, Pipe.sizeOf(RawDataSchema.instance, RawDataSchema.MSG_CHUNKEDSTREAM_1));
		           Pipe.readNextWithoutReleasingReadLock(input);
				              	           
		           while (processAvail()){}
		           		           
		           
		        break;
		        case -1:
		           pendingShutdown = true;
		           Pipe.confirmLowLevelRead(input, Pipe.EOF_SIZE);	
		           Pipe.releaseReadLock(input);		    
		        break;
		    }
		}
		
	}

	
	private boolean processAvail() {
		int len = inputStream.available();
		if (!encrypt) {
			if (len<blockSize) {
				return false;//need data to decrypt
			}
		} else {
			if (len<=0) {
				return false;//need data to encrypt
			}
		}

		int availSourceLen = Math.min(len, sourceMask-(inputStream.absolutePosition() & sourceMask));

		//compute target size, round up to next passSize (block size)
		int targetSize = ((availSourceLen+passSizeMask)>>passSizeBits)<<passSizeBits;
		
		int availTargetSize = Math.min(targetSize,
				                       Math.min(output.maxVarLen, 
				                    		    targetMask-(Pipe.getWorkingBlobHeadPosition(output)&targetMask) ));
		
		if (availTargetSize < targetSize) {
			//must lower source since the target got limited.
			availSourceLen = availTargetSize-passSize;
		}

		//For most common case this conditional will be true
		if (Pipe.hasRoomForWrite(output) ) {		
			int size= Pipe.addMsgIdx(output, RawDataSchema.MSG_CHUNKEDSTREAM_1);
			
			try {
				//data must be in block size units 
				//NOTE: block is always a power of 2 therefore it must go int buffer and we will never have a 
				//      small part left at the end of the array.

				int encSize = c.update(
										  sourceBuffer, 
										  inputStream.absolutePosition() & sourceMask, 
										  availSourceLen, 
										  targetBuffer, 
										  Pipe.getWorkingBlobHeadPosition(output));
					
				assert(LOCUtil.isLocOfAnyType(fieldLOC, TypeMask.TextASCII, TypeMask.TextASCIIOptional, TypeMask.TextUTF8, TypeMask.TextUTF8Optional, TypeMask.ByteVector, TypeMask.ByteVectorOptional)): "Value found "+LOCUtil.typeAsString(fieldLOC);

				Pipe.addBytePosAndLen(output, Pipe.getWorkingBlobHeadPosition(output), encSize);
				Pipe.addAndGetBytesWorkingHeadPosition(output, encSize);
												
				inputStream.skip(availSourceLen);

				Pipe.releasePendingAsReadLock(input, availSourceLen);
				
			} catch (Exception e) {
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
