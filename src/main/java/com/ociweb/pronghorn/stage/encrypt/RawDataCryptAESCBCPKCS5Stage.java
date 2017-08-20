package com.ociweb.pronghorn.stage.encrypt;

import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.pipe.token.LOCUtil;
import com.ociweb.pronghorn.pipe.token.TypeMask;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class RawDataCryptAESCBCPKCS5Stage extends PronghornStage {

	public static final Logger logger = LoggerFactory.getLogger(RawDataCryptAESCBCPKCS5Stage.class);
	
	private static final int REQ_OUT_SIZE = 2*Pipe.sizeOf(RawDataSchema.instance, RawDataSchema.MSG_CHUNKEDSTREAM_1);
	private static final int SIZE_OF_CHUNKED = Pipe.sizeOf(RawDataSchema.instance, RawDataSchema.MSG_CHUNKEDSTREAM_1);
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
	
	private boolean pendingShutdown = false;
	
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
		
		//logger.info("encrypt {} for input pipe {} ",encrypt, input.id);
		
		
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

	@Override
	public void shutdown() {
		//logger.info("shutdown");
	}
	
	@Override
	public void run() {
		
		if (pendingShutdown) {
			assert(!Pipe.hasContentToRead(input)) : "Should not have more data after shutdown request";
			//this pattern eliminates need for spin-lock which can cause unit test to hang.
			if (Pipe.hasRoomForWrite(output)) {
				 Pipe.publishEOF(output);
				 requestShutdown();
			}
			return;
		}

		while (processAvail()) {}
		
		//logger.info("encrypt {} has content {}",encrypt, Pipe.hasContentToRead(input));
		
		while (Pipe.hasContentToRead(input) 
		   	   && Pipe.hasRoomForWrite(output, REQ_OUT_SIZE)) {
	    
			int msgIdx = Pipe.takeMsgIdx(input);
		    switch(msgIdx) {
		        case RawDataSchema.MSG_CHUNKEDSTREAM_1:
		           int nxtLen = Pipe.peekInt(input, 1);
		           
		           //logger.info("crypt data reading {} encrypt {} from pipe {}",nxtLen, encrypt, input.id);
		           //when decrypting we need to accum work togetehr as needed
		           //but when encrypting we will consume all the data of each block
		           
		           inputStream.accumLowLevelAPIField();
		           
		           
		           if (nxtLen<0) {
		        	    
		        	    //clean up before we move on
		        	    while (processAvail()){}
		        	   		        	   
						try {
						//	logger.info("reset for new read from pipe {}", input.id);
							
							iv = new byte[passSize];
							c.init(encrypt ? Cipher.ENCRYPT_MODE : Cipher.DECRYPT_MODE, 
									   new SecretKeySpec(pass, "AES"),
									   new IvParameterSpec(iv));
							
							//byte[] lastBlock = c.doFinal();							
							//System.err.println("hello "+lastBlock.length);
						} catch (Exception e) {
							throw new RuntimeException(e);
						} 
						
						int s = Pipe.addMsgIdx(output, RawDataSchema.MSG_CHUNKEDSTREAM_1);
						Pipe.addNullByteArray(output);
						Pipe.confirmLowLevelWrite(output, s);
						Pipe.publishWrites(output);
						
						Pipe.confirmLowLevelRead(input, SIZE_OF_CHUNKED);
				        Pipe.releaseReadLock(input);	
		        	   
		           } else {
		           
			           Pipe.confirmLowLevelRead(input, SIZE_OF_CHUNKED);
			           Pipe.readNextWithoutReleasingReadLock(input);
					       
			           while (processAvail()){}
			           		           
		           }
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
		int availSourceLen = Math.min(len, sourceMask-(inputStream.absolutePosition() & sourceMask));

		if (availSourceLen <= 0) {
			return false;//need data to encrypt
		}

		/////////////////
		//is this needed?
		/////////////////
		//compute target size, round up to next passSize (block size)
		int targetSize = ((availSourceLen+passSizeMask)>>passSizeBits)<<passSizeBits;
		
		int availTargetSize = Math.min(targetSize,
				                       Math.min(output.maxVarLen, 
				                    		    targetMask-(Pipe.getWorkingBlobHeadPosition(output)&targetMask) ));
		
		if (availTargetSize < targetSize) {
			//must lower source since the target got limited.
			availSourceLen = availTargetSize-passSize;
		}
		//////////
		//////////

		//For most common case this conditional will be true
		if (Pipe.hasRoomForWrite(output) ) {		
			
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
				
				//only write out a block when we have data 
				
				if (encSize>0) {
					
					int size= Pipe.addMsgIdx(output, RawDataSchema.MSG_CHUNKEDSTREAM_1);
					
//					logger.info("pipe {} encryp {} output size {} orig source size {} ",
//							DataInputBlobReader.getBackingPipe(inputStream).id, 
//							encrypt,
//							encSize,
//							availSourceLen);
					
					assert(LOCUtil.isLocOfAnyType(fieldLOC, TypeMask.TextASCII, TypeMask.TextASCIIOptional, TypeMask.TextUTF8, TypeMask.TextUTF8Optional, TypeMask.ByteVector, TypeMask.ByteVectorOptional)): "Value found "+LOCUtil.typeAsString(fieldLOC);
	
					Pipe.addBytePosAndLen(output, Pipe.getWorkingBlobHeadPosition(output), encSize);
					Pipe.addAndGetBytesWorkingHeadPosition(output, encSize);
					Pipe.confirmLowLevelWrite(output, size);
					Pipe.publishWrites(output);
													
					inputStream.skip(availSourceLen);
	
					Pipe.releasePendingAsReadLock(input, availSourceLen);
				} else {
					//logger.info("{} not writing enc {} due to no data {}",stageId, encrypt, availSourceLen);
				}
			} catch (Exception e) {
				throw new RuntimeException(encrypt?"failure while encrypting":"failure while decrypting",e);
			} 
						
						
			return true;
		} else {
			return false;//no room to do work
		}
	}

	
}
