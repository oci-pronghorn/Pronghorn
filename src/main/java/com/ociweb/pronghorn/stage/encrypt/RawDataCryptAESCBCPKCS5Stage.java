package com.ociweb.pronghorn.stage.encrypt;

import java.security.AlgorithmParameters;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.file.schema.BlockStorageReceiveSchema;
import com.ociweb.pronghorn.stage.file.schema.BlockStorageXmitSchema;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.Appendables;

public class RawDataCryptAESCBCPKCS5Stage extends PronghornStage {

	public static final Logger logger = LoggerFactory.getLogger(RawDataCryptAESCBCPKCS5Stage.class);
	
	private static final int REQ_OUT_SIZE = 2*Pipe.sizeOf(RawDataSchema.instance, RawDataSchema.MSG_CHUNKEDSTREAM_1);
	private static final int SIZE_OF_CHUNKED = Pipe.sizeOf(RawDataSchema.instance, RawDataSchema.MSG_CHUNKEDSTREAM_1);
	private byte[] iv;
	private Cipher c;
	
	private final byte[] pass;
	private final Pipe<RawDataSchema> input;
	private final Pipe<RawDataSchema> output;

	private final Pipe<BlockStorageReceiveSchema> finalInput;
	private final Pipe<BlockStorageXmitSchema> finalOutput;
     
	private byte[] sourceBuffer;
	private int sourceMask;
	private int blockSize;
	
	private int targetMask;
	
	private final boolean encrypt;
	
	private final int passSizeBits = 4;
	private final int passSize = 1<<passSizeBits;
	
	private DataInputBlobReader<RawDataSchema> inputStream;
	
	private boolean pendingShutdown = false;
	private int activeShuntPosition = 0;
	
	private SecretKeySpec sks;
	private IvParameterSpec ips;
	
	private byte[] targetBuffer;	

	private long filePosition;
	
	private boolean finalAckPending = false;

	private byte[] tempWrapSpace = new byte[2*passSize];
	
	private long finalPosA;
	private int finalLenA;
	private byte[] finalDataA;
	private long finalPosB;
	private int finalLenB;
	private byte[] finalDataB;
		
	//required at the start of each file to ensure the same data is never used to begin files
	private final int firstBlockBytes = passSize*2; //two blocks is more than sufficient
	private final byte[] firsBlockBytesBacking = new byte[firstBlockBytes];
	
	//only used for double buffering of final value when we wrap off the end of the ring.
	private final int finalBlockSize = 8+4+(passSize*2);//file position + size + 2 blocks 
	//NOTE: in the future we want to expand the block size and add CRC check values to confirm data was not replaced.
	
	private final byte[] finalWrapBuffer = new byte[2 * finalBlockSize];
	
	private static SecureRandom random;
	static {
		try {
			random = SecureRandom.getInstanceStrong();			
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
	}
	
	
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
				                         Pipe<RawDataSchema> output,
				                         //both decrypt and encrypt instances must point to same file
				                         Pipe<BlockStorageReceiveSchema> finalInput,
				                         Pipe<BlockStorageXmitSchema> finalOutput
				               
			) {
		super(graphManager, join(input, finalInput), join(output, finalOutput));
		if (pass.length!=passSize) {
			throw new UnsupportedOperationException("pass must be "+passSize+" bytes");
		}
		this.encrypt = encrypt;
		this.pass = pass;
		this.input = input;
		this.output = output;
		this.finalInput = finalInput;
		this.finalOutput = finalOutput;
		
		//logger.info("encrypt {} for input pipe {} ",encrypt, input.id);
		
	}

	
	@Override
	public void startup() {
		
		this.targetBuffer = Pipe.blob(output);
		this.targetMask = Pipe.blobMask(output);
		this.sourceBuffer = Pipe.blob(input);
		this.sourceMask = Pipe.blobMask(input);
		this.inputStream = Pipe.inputStream(input);

		this.finalPosA = -1;
		this.finalDataA = new byte [2*passSize];
		this.finalPosB = -1;
		this.finalDataB = new byte [2*passSize];
				
		
		try {
			
			c = Cipher.getInstance("AES/CBC/PKCS5Padding");
			iv = new byte[passSize];
			
			System.arraycopy(pass, 0, iv, 0, 16);
			
			
			sks = new SecretKeySpec(pass, "AES");
			ips = new IvParameterSpec(iv);
			c.init(encrypt ? Cipher.ENCRYPT_MODE : Cipher.DECRYPT_MODE, sks, ips);

			blockSize = c.getBlockSize();
			
			assert(blockSize <= input.sizeOfBlobRing) : "block size must be smaller or equal to blob size";
			assert(0 == (input.sizeOfBlobRing%blockSize)) : "block size must fit into blob evently";
			
			//////////////////////////////////////////////
			//this is a demonstration of reading iv
			/////////////////////////////////////////////
			//AlgorithmParameters params = c.getParameters();
			//params.getParameterSpec(paramSpec)
			//ivBytes = params.getParameterSpec(IvParameterSpec.class).getIV()
			//////////////////////////////////////////////
			
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
			if (!inputStream.hasRemainingBytes()) {				
				assert(!Pipe.hasContentToRead(input)) : "Should not have more data after shutdown request";
				//this pattern eliminates need for spin-lock which can cause unit test to hang.
				if (Pipe.hasRoomForWrite(output)) {
					 Pipe.publishEOF(output);
					 requestShutdown();
				}
				return;
			}
		}

		processFinalInputResponse();
		
		while (processAvail()) {}	
		
		if (encrypt && inputStream.hasRemainingBytes()) {
			return;//do not continue until encryption of current block is completed
		}
		
		//logger.info("encrypt {} has content {}",encrypt, Pipe.hasContentToRead(input));
		
	    while ((!finalAckPending)//do not pick up new data if we are waiting for previous write.
	    	 && Pipe.hasContentToRead(input) 
		     && Pipe.hasRoomForWrite(output, REQ_OUT_SIZE)) {
	    	
				int msgIdx = Pipe.takeMsgIdx(input);
			    switch(msgIdx) {
			        case RawDataSchema.MSG_CHUNKEDSTREAM_1:
			           int nxtLen = Pipe.peekInt(input, 1);
			           
			           inputStream.accumLowLevelAPIField();
			           			           
			           if (nxtLen<0) {
			        	   logger.info("processing end of data stream for encrypt:{}",encrypt);
			        	   if (!encrypt) {
			        		   try {
								Thread.sleep(2000);
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
			        	   }
			        	   //TODO: the A and B values are missing!!!!
			        	   
			        	   //clean up before we move on
			        	   while (processAvail()){}
			        	   
			        	    if (!encrypt) {
			        	    	logger.info("decrypt found end of data with posA "+finalPosA+" and posB "+finalPosB+" current pos "+filePosition);
			        	    	int len = 0;
			        	    	int targetPos = Pipe.getWorkingBlobHeadPosition(output);
			        	    	if (filePosition == finalPosA) {
			        	    		len = processBlock(finalLenA, 0, finalDataA, //TODO: this constant comes from where?
							                    targetPos, targetBuffer, targetMask);

			        	    		logger.info("found match at postion A final length added "+len+" from "+finalDataA.length);
			        	    		
			        	    		Appendables.appendUTF8(System.out, targetBuffer, Pipe.getWorkingBlobHeadPosition(output), len, targetMask);
			        	    		
			        	    		publishBockOut(targetPos, len);
			        	    		
			        	    		
			        	    	} else if (filePosition == finalPosB) {
			        	    		logger.info("found match at postion B");
			        	    		len = processBlock(finalLenB, 0, finalDataB,
			        	    				    targetPos, targetBuffer, targetMask);
			        	    	} else {			        	    		
			        	    		logger.info("!!!! Warning, unable to read tail, no positions matched {} "+filePosition);
			        	    	}
			        	    	
			        	    //	len += doFinalIntoRing(targetBuffer, targetPos+len, targetMask);
			        	    	publishBockOut(targetPos, len);
			        	    	
			        	    } 
			        	    
			        	    try {
								//NOTE: this must be called upon the detection of -1
								Arrays.fill(iv, (byte)0);
								c.init(encrypt ? Cipher.ENCRYPT_MODE : Cipher.DECRYPT_MODE, sks, ips);
							} catch (Exception e) {
								throw new RuntimeException(e);
							} 
							
			        	    //////////////////
			        	    //send end of data marker downstream
			        	    //////////////////
							int s = Pipe.addMsgIdx(output, RawDataSchema.MSG_CHUNKEDSTREAM_1);
							Pipe.addNullByteArray(output);
							Pipe.confirmLowLevelWrite(output, s);
							Pipe.publishWrites(output);
							
							Pipe.confirmLowLevelRead(input, SIZE_OF_CHUNKED);
					        Pipe.releaseReadLock(input);
					        
					        filePosition = 0;
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


	private void processFinalInputResponse() {
		while (PipeReader.tryReadFragment(finalInput)) {
		    int msgIdx1 = PipeReader.getMsgIdx(finalInput);
		    switch(msgIdx1) {
		        case BlockStorageReceiveSchema.MSG_ERROR_3:
		        	long fieldPosition2 = PipeReader.readLong(finalInput,BlockStorageReceiveSchema.MSG_ERROR_3_FIELD_POSITION_12);
		        	StringBuilder fieldMessage = PipeReader.readUTF8(finalInput,BlockStorageReceiveSchema.MSG_ERROR_3_FIELD_MESSAGE_10,new StringBuilder(PipeReader.readBytesLength(finalInput,BlockStorageReceiveSchema.MSG_ERROR_3_FIELD_MESSAGE_10))); //TODO: error??
		            
				break;
		        case BlockStorageReceiveSchema.MSG_WRITEACK_2:
		        	final long fieldPosition = PipeReader.readLong(finalInput,BlockStorageReceiveSchema.MSG_WRITEACK_2_FIELD_POSITION_12);
		        	
		        	logger.info("XXXXXX ack for write encyprt: {}  position: {} ",encrypt, fieldPosition);
		        	
		        	finalAckPending = false;
		        break;
		        case BlockStorageReceiveSchema.MSG_DATARESPONSE_1:
		        	assert(!encrypt) : "did not expect this message, should only be when decrypting";
		        	//this is the replay for the decryptor
		        	long fieldPosition1 = PipeReader.readLong(finalInput,BlockStorageReceiveSchema.MSG_DATARESPONSE_1_FIELD_POSITION_12);
		        	
		        	//TODO: in place, apply xor to pull back in our tail data
		        	
		        	DataInputBlobReader<BlockStorageReceiveSchema> fieldPayload = PipeReader.inputStream(finalInput, BlockStorageReceiveSchema.MSG_DATARESPONSE_1_FIELD_PAYLOAD_11);
		        	assert(0 == fieldPosition1);
		        	assert((2*finalBlockSize)  == fieldPayload.available());
		        	
		        	//this data is needed for decrypt when we reach the end
		        	finalPosA = fieldPayload.readLong();
		        	
		        	logger.info("XXXXXX result for read encyprt: {}  position: {} from file loc:{}",encrypt, finalPosA, fieldPosition1);
		        	
		        	
		        	finalLenA = fieldPayload.readInt();
		        	fieldPayload.read(finalDataA);
		        	
		        	finalPosB = fieldPayload.readLong();
		        	finalLenB = fieldPayload.readInt();
		        	fieldPayload.read(finalDataB);
		        			        	
		        break;
		        case -1:
		        	//TODO: should only do if we sent it first to finalOutput
		            //requestShutdown();
		        break;
		    }
		    PipeReader.releaseReadLock(finalInput);
		}
	}

	
	private boolean processAvail() {
		
		if (0 == filePosition) {
			if (!encrypt) {
				
				//consume header block since it was just random data to protect the file
				if (Pipe.hasRoomForWrite(finalOutput)&&
					inputStream.hasRemainingBytes() &&
					inputStream.available() > firstBlockBytes &&
					Pipe.hasRoomForWrite(output)) {
		
					
					logger.info("XXXXXX requested read encyprt: {}  position: 0 size: {} ",encrypt, 2*finalBlockSize);

					BlockStorageXmitSchema.publishRead(finalOutput, 0, 2*finalBlockSize);
									
					final int targetPos = Pipe.getWorkingBlobHeadPosition(output);
					
					//this call will grow fielPosition
					int len = processBlock(inputStream.available(), 
							     inputStream.absolutePosition() & sourceMask, sourceBuffer,
							     targetPos, targetBuffer, targetMask);
					
					//copy needed data
					System.arraycopy(targetBuffer, targetPos, firsBlockBytesBacking, 0, firstBlockBytes);
					inputStream.skip(inputStream.available());
					
					int rem = len-firstBlockBytes;
					if (rem>0) {
						//must write this out
						System.arraycopy(targetBuffer, targetPos+firstBlockBytes, 
										 targetBuffer, targetPos, 
								         rem);
						
						publishBockOut(targetPos, rem);
						
						if (!encrypt) {
							Appendables.appendUTF8(System.out, targetBuffer, targetPos, rem, targetMask);	
							System.out.println();
						}
						
						
						return true;
					}
					
				} else {
					return false;
				}
			}
		}
		
		if (Pipe.hasRoomForWrite(output) &&
			inputStream.hasRemainingBytes()) {
			//can not wrap off end so must limit
			final int rollLimit = sourceMask-(inputStream.absolutePosition() & sourceMask);
			assert(rollLimit>0);
			int avail = inputStream.available();
			
			
			if (avail <= rollLimit) {						
				
				int targetPos = Pipe.getWorkingBlobHeadPosition(output);
				
				int encSize = 0;
				if (encrypt && 0==filePosition) {
					
					if (avail <= passSize) { //TODO: this must work with 1 byte of data...
						//one case where we need to read more for encrypt...
						return false;
					}
					
					//this is added here so it is part of the same write block
 					int len = processBlock(firstBlockBytes, 0, firsBlockBytesBacking, 
 											targetPos, targetBuffer, targetMask);
 					
 					encSize += len;
 					targetPos += len;
				}				
				
				//debug what was encrypted
				if (encrypt) {
					StringBuilder text = new StringBuilder("Encrypt: ");
					Appendables.appendUTF8(text, sourceBuffer, inputStream.absolutePosition(), avail, sourceMask);
					System.out.println(text.toString());
				}
								
				final int tSize = processBlock(avail, inputStream.absolutePosition() & sourceMask, sourceBuffer,
						                       targetPos, targetBuffer, targetMask); 
				encSize += tSize;
				
				//debug what was decrypted
				if (!encrypt) {
					Appendables.appendUTF8(System.out, targetBuffer, targetPos, tSize, targetMask);	
					System.out.println();
				}
								
				if (encSize>0) {
					assert(tSize!=0) : "Must send block of real data with headers "+encSize;
					int origTargetPos = Pipe.getWorkingBlobHeadPosition(output);
					publishBockOut(origTargetPos,encSize);					
				} else {
					logger.info("ERROR ENC SIZE IS ZERO................");
				}
				
				inputStream.skip(avail);
			} else {
				//encrypt both blocks into target.
				System.err.println("rollover tested");
				
				final int blobPos = Pipe.getWorkingBlobHeadPosition(output);
				
				int encSize = processBlock(rollLimit, inputStream.absolutePosition() & sourceMask, sourceBuffer, 
											blobPos, targetBuffer, targetMask); 
				inputStream.skip(rollLimit);
				Pipe.addAndGetBytesWorkingHeadPosition(output, encSize);
				
				final int rem = avail-rollLimit;
				int block2 = processBlock(rem, inputStream.absolutePosition() & sourceMask, sourceBuffer,
						                Pipe.getWorkingBlobHeadPosition(output), targetBuffer, targetMask); 
				encSize += block2;
				inputStream.skip(rem);
				Pipe.addAndGetBytesWorkingHeadPosition(output, block2);
				
				if (encSize>0) {
					final int size= Pipe.addMsgIdx(output, RawDataSchema.MSG_CHUNKEDSTREAM_1);
					Pipe.addBytePosAndLen(output, blobPos, encSize);
					Pipe.confirmLowLevelWrite(output, size);
					Pipe.publishWrites(output);
				}
			}
			
			Pipe.releasePendingAsReadLock(input, avail);
			
			return true;
		} else {
			return false;
		}
	}


	private void publishBockOut(final int targetPos, int length) {
		int size= Pipe.addMsgIdx(output, RawDataSchema.MSG_CHUNKEDSTREAM_1);
		Pipe.addBytePosAndLen(output, targetPos, length);
		Pipe.addAndGetBytesWorkingHeadPosition(output, length);
		Pipe.confirmLowLevelWrite(output, size);
		Pipe.publishWrites(output);
	}
	
	int ttt = 0;

	private int processBlock(int avail, int availPos, byte[] availBacking, 
			                 int targetPos, byte[] targetBacking, int targetMask) {
		assert(targetBacking.length>32);
		try {
			
			    int requiredOutputRoom = c.getOutputSize(avail);
			    
			    int roomBeforeWrap = targetBacking.length-targetPos;
			    
			    int result;
			    if (requiredOutputRoom <= roomBeforeWrap) {
					result = c.update(
											availBacking, 
											availPos, 
											avail, 
											targetBacking, 
											targetPos);
					
					if (!encrypt) {
						System.err.println("reading result "+result+
								           " for target "+targetBacking.length+
								           " avail "+avail);
					}
					
			    } else {
			    	System.out.println("tested when no room");
			    	////////////////////////////////////////
			    	//this conditional is only needed because we may not have all the room
			    	//before the wrap and because c.update does not have the right signature to suppor this.
			    	/////////////////////////////////////
			    	if (requiredOutputRoom > tempWrapSpace.length) {
			    		tempWrapSpace = new byte[requiredOutputRoom*2];
			    	}
					result = c.update(
											availBacking, 
											availPos, 
											avail, 
											tempWrapSpace, 
											0);
															
					Pipe.copyBytesFromToRing(tempWrapSpace, 0, Integer.MAX_VALUE, 
							targetBacking, targetPos, targetMask,
							result);
			    	
			    }
				
			    final long startPos = filePosition;
			    
			    filePosition += result;
			    
				if (targetPos+result-16 > 0) {
										
					//for encrypt we want to do final but only after we have written the first block bytes
					if (encrypt && (startPos >= firstBlockBytes) ) {
						
						//These bytes were sent to final so we must keep them for the next block
						int extraBytesToReAdd = (ttt+avail)%passSize;
						
						//NOTE: this final may be written before or after end of body.
						//      if after body will just use old end
						//      if before body will continue to use old end
						//      it is critical that we only have 1 outstanding at a time.
						//      so if we have an outstanding ack no new work should be taken.
						publishDoFinal();
								

						//take last 16 and keep it for the new vi
						System.arraycopy(targetBuffer, targetPos+result-16, iv, 0, 16);
						ips = new IvParameterSpec(iv);						
						c.init(encrypt ? Cipher.ENCRYPT_MODE : Cipher.DECRYPT_MODE, sks, ips);
						
						
						StringBuilder temp = new StringBuilder("** Missing bytes added: ");
						Appendables.appendUTF8(temp, availBacking, 
												(availPos+avail)-extraBytesToReAdd, 
												extraBytesToReAdd, Integer.MAX_VALUE);
						System.out.println(temp);
						
						
						int shouldBeZero = c.update(
													availBacking, 
													(availPos+avail)-extraBytesToReAdd, 
													extraBytesToReAdd, 
													tempWrapSpace, 
													0);
						assert(0 == shouldBeZero);
						
						ttt += extraBytesToReAdd;
						
					
					}
				}
				return result;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	//TODO: confirm that we write a zero lengh block when data does not flush 16... it is in the final.

	private void publishDoFinal() throws IllegalBlockSizeException, ShortBufferException, BadPaddingException {

		assert(encrypt) : "never call for decrypt";
		
		//TODO: must add xor of this to ensure not repeated key usage is let out.
		
		
		PipeWriter.presumeWriteFragment(finalOutput, BlockStorageXmitSchema.MSG_WRITE_1);
		PipeWriter.writeLong(finalOutput,
							BlockStorageXmitSchema.MSG_WRITE_1_FIELD_POSITION_12, 
							(activeShuntPosition*finalBlockSize));
		
		activeShuntPosition = 1&(activeShuntPosition+1);//toggle
		
		byte[] blob = Pipe.blob(finalOutput);						
		final int origPos = Pipe.getWorkingBlobHeadPosition(finalOutput);
	
		DataOutputBlobWriter.write64(blob, finalOutput.blobMask, origPos, filePosition);
		final int lenPos = origPos+8;					
		
		int finalOffset = finalOutput.blobMask & (lenPos+4);
		int finalMask = finalOutput.blobMask;
		
		int finalLength = doFinalIntoRing(blob, finalOffset, finalMask);			
		
		DataOutputBlobWriter.write32(blob, finalOutput.blobMask, lenPos, finalLength);
		
		//we write full block to ensure file is filled up to that size.
		PipeWriter.writeSpecialBytesPosAndLen(finalOutput, 
				BlockStorageXmitSchema.MSG_WRITE_1_FIELD_PAYLOAD_11,
				finalBlockSize, origPos);
								
		PipeWriter.publishWrites(finalOutput);
		

    	logger.info("XXXXXX final write encyprt: {}  position: {} ",encrypt, filePosition);
    	
		
		assert(!finalAckPending) : "internal logic error";
		finalAckPending = true; 		
	}


	private int doFinalIntoRing(byte[] targetBacking, int targetPosition, int targetMask) {
		int finalLength = -1;
		try{ 
			if (finalOutput.sizeOfBlobRing-targetPosition >= (2*passSize)) { //is never more than 2 blocks
				finalLength = c.doFinal(targetBacking, targetPosition );
				assert(finalLength <= (finalBlockSize-8));							
			} else {
				//////////////////////////////
				//the final must be written out but the target buffer has wrapped
				//in the middle and the c.doFinal does not support the right method 
				//signature to support this case, we must copy to the local buffer first
				/////////////////////////////
				finalLength = c.doFinal(finalWrapBuffer, 0);
				assert(finalLength <= (finalBlockSize-8));
				Pipe.copyBytesFromToRing(finalWrapBuffer, 0, Integer.MAX_VALUE, 
						                 targetBacking, targetPosition, targetMask,
						                 finalLength);
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return finalLength;
	}

	
}
