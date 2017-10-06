package com.ociweb.pronghorn.stage.encrypt;

import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Random;

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
import com.ociweb.pronghorn.pipe.util.hash.MurmurHash;
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
	private final int passMask = passSize-1;
	
	private DataInputBlobReader<RawDataSchema> inputStream;
	
	private boolean pendingShutdown = false;
	private int activeShuntPosition = 0;
	
	private SecretKeySpec sks;
	private IvParameterSpec ips;
	
	private byte[] targetBuffer;
	private long fileOutputPosition;
	
	private boolean finalAckPending = false;
	private byte[] tempWrapSpace = new byte[2*passSize];
	
	private long finalPos;
	private int finalLen;
	private byte[] finalData;
		
	
	//only used for double buffering of final value when we wrap off the end of the ring.
	private final int finalBlockSize = 4*passSize;//bumped up to power of 2 8+4+(passSize*2);//file position + size + 2 blocks 
	//NOTE: in the future we want to expand the block size and add CRC check values to confirm data was not replaced.
	private final int guidBlockSize = finalBlockSize*2;

	private final int safeOffset = finalBlockSize;

	//required at the start of each file to ensure the same data is never used to begin files
	private final byte[] firstBlockBytesBacking = new byte[guidBlockSize];

	boolean isFinalRequested; //blocks EOF from getting read until after we consume the tail data
	
	
	//two blocks of rolling hash which is why we also have 2 blocks of random data at the start
	private final byte[] rollingHash = new byte[guidBlockSize];
	private final int    rollingHashMask = guidBlockSize-1;
	private int    rollingHashInputPos = 0;
	
	
	
	private final byte[] finalWrapBuffer = new byte[guidBlockSize];

	private static Random fileHeaderGenerator;
	static {
		//for testing this can be used...
		//fileHeaderGenerator = new Random(123); 
		
		//use this block for production
		fileHeaderGenerator = new SecureRandom();

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

	}

	public String toString() {
		String parent = super.toString();
		if (encrypt) {
			return parent+" encrypt";
		} else {
			return parent+" decrypt";
		}		
	}
	
	//TODO: needs test to confirm that zero lenght block writes continues to work
	
	//TODO: needs to add recovery mode for loading corrupt files
	//      on recover we must read the final blocks first then read every byte from
	//      the beginning, at each value we must check if one final matches
	//      upon finding the matching final then trim the remaining of the file.
	
	
	@Override
	public void startup() {
		
		this.targetBuffer = Pipe.blob(output);
		this.targetMask = Pipe.blobMask(output);
		this.sourceBuffer = Pipe.blob(input);
		this.sourceMask = Pipe.blobMask(input);
		this.inputStream = Pipe.inputStream(input);

		this.finalPos = -1;
		this.finalData = new byte [finalBlockSize];
		
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
	    	 && (!isFinalRequested) 
	    	 && Pipe.hasContentToRead(input) 
		     && Pipe.hasRoomForWrite(output, REQ_OUT_SIZE)) {
	    	
				int msgIdx = Pipe.takeMsgIdx(input);
			    switch(msgIdx) {
			        case RawDataSchema.MSG_CHUNKEDSTREAM_1:
			           int nxtLen = Pipe.peekInt(input, 1);
			           
			           inputStream.accumLowLevelAPIField();
			           			           
			           if (nxtLen<0) {
			        	   //logger.trace("processing end of data stream for encrypt:{}",encrypt);
			        	   
			        	   //clean up before we move on
			        	   while (processAvail()){}
			        	   
			        	    if (!encrypt) {
			        	    	
			        	    	assert(!inputStream.hasRemainingBytes()) : "all bytes must be consumed before doing final.";
			        			//do not process any more input until this decrypt has its final block
			        	    	isFinalRequested = true;
								//reqeust the stored final blocks so we can find which is needed.
							    assert(guidBlockSize>0) : "must have some length";
								PipeWriter.presumeWriteFragment(finalOutput, BlockStorageXmitSchema.MSG_READ_2);
								PipeWriter.writeLong(finalOutput,BlockStorageXmitSchema.MSG_READ_2_FIELD_POSITION_12, (long) 0);
								PipeWriter.writeInt(finalOutput,BlockStorageXmitSchema.MSG_READ_2_FIELD_READLENGTH_10, guidBlockSize);
								PipeWriter.publishWrites(finalOutput);			        	    	
		        	    		//file position for decrypt is cleared after we get the final blocks
							    
							    							    
			        	    } else { 
			        	    	//for encrypt we can reset the counters here 
			        	    	resetToBeginning();
			        	    }
							
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


	private void resetToBeginning() {
		fileOutputPosition = 0;
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
	}


	private void processFinalInputResponse() {
		while (PipeReader.tryReadFragment(finalInput) && PipeWriter.hasRoomForWrite(output)) {
		    int msgIdx1 = PipeReader.getMsgIdx(finalInput);
		    switch(msgIdx1) {
		        case BlockStorageReceiveSchema.MSG_ERROR_3:
		        	long fieldPosition2 = PipeReader.readLong(finalInput,BlockStorageReceiveSchema.MSG_ERROR_3_FIELD_POSITION_12);
		        	StringBuilder fieldMessage = PipeReader.readUTF8(finalInput,BlockStorageReceiveSchema.MSG_ERROR_3_FIELD_MESSAGE_10,new StringBuilder(PipeReader.readBytesLength(finalInput,BlockStorageReceiveSchema.MSG_ERROR_3_FIELD_MESSAGE_10))); //TODO: error??
		            
				break;
		        case BlockStorageReceiveSchema.MSG_WRITEACK_2:
		        	final long fieldPosition = PipeReader.readLong(finalInput,BlockStorageReceiveSchema.MSG_WRITEACK_2_FIELD_POSITION_12);
		        	
		        	//logger.info("ack for write encyprt: {}  position: {} ",encrypt, fieldPosition);
		        	
		        	finalAckPending = false;
		        break;
		        case BlockStorageReceiveSchema.MSG_DATARESPONSE_1:
		        	assert(!encrypt) : "did not expect this message, should only be when decrypting";
		        	
		        	//this is the replay for the decryptor
		        	long fieldPosition1 = PipeReader.readLong(finalInput,BlockStorageReceiveSchema.MSG_DATARESPONSE_1_FIELD_POSITION_12);
		        	
		        	DataInputBlobReader<BlockStorageReceiveSchema> fieldPayload = PipeReader.inputStream(
		        			            finalInput, BlockStorageReceiveSchema.MSG_DATARESPONSE_1_FIELD_PAYLOAD_11);

		        	
		        	int blockPos = fieldPayload.absolutePosition();
		           	byte[] blockBack = finalInput.blobRing;
		        	int blockMask = finalInput.blobMask;
		        			        	
		        	assert(0 == fieldPosition1);
		        	assert(guidBlockSize  == fieldPayload.available());

		        	int avail = fieldPayload.available();
		        	
		        	int offset = (int)((fileOutputPosition-safeOffset) & rollingHashMask);		        	
			        Pipe.xorBytesToBytes(rollingHash, offset, rollingHashMask,
			        		             blockBack, blockPos, blockMask, finalBlockSize);
		
					//Appendables.appendArray(Appendables.appendValue(System.out, offset), 
					//		rollingHash, offset, rollingHashMask, 64 ).append(" decrypt\n ");

		        	//this data is needed for decrypt when we reach the end
		        	finalPos = fieldPayload.readLong();
		        	finalLen = fieldPayload.readInt();

		    		final int headerBytesToIgnore = (int)(guidBlockSize-fileOutputPosition);
		    		
		        	
		        	if (fileOutputPosition == finalPos 
		        		&& finalLen>=0
		        		&& finalLen<=finalData.length
		        		&& (0==(passMask&finalLen))  ) {
		        	
		        		fieldPayload.read(finalData, 0, finalLen);
		        		
		        		activeShuntPosition = 1; //replace the second slot first since we use first
		        	
		        	} else {
	
		        		offset = (int)((fileOutputPosition-safeOffset) & rollingHashMask);
		        		Pipe.xorBytesToBytes(rollingHash, offset, rollingHashMask, blockBack, blockPos+finalBlockSize, blockMask, finalBlockSize);
		        
		        		//Appendables.appendArray(Appendables.appendValue(System.out, offset), 
		        		//		rollingHash, offset, rollingHashMask, 64 ).append(" decrypt\n ");
		        		
		        		fieldPayload.skipBytes(finalBlockSize-(8+4));//align to the next needed block.
		        		
		        		finalPos = fieldPayload.readLong();
		        		finalLen = fieldPayload.readInt();
		        			        				        	
			        	//only take this value if it matches the expectation
			        	if (fileOutputPosition == finalPos
			        			&& finalLen>=0
				        		&& finalLen<=finalData.length
				        		&& (0==(passMask&finalLen))  	) {
			        		
			        		fieldPayload.read(finalData, 0, finalLen);
			        		
			        		activeShuntPosition = 0; //replace the first slot first since we use second
			        		
			        	} else {
			        			        		
			        		if (headerBytesToIgnore>0) {
			        			//this data is not valid for now...

			        			logger.info("not yet started must skip {} ", headerBytesToIgnore);
			        			
			        		} else {
			        						        		
			        			//neither position matched,  corrupt data discovered, should return -2 length
			        		
				        		logger.info("Warning: looking for data at position {} found no match in {} length {} ", 
				        				fileOutputPosition, finalPos, finalLen);
				        		
				        		finalPos = -1;			        		
			        		}
			        		
			        	}
		        	}    	    	
    
        	    	assert(finalPos != -1) : "must have matching position by this point in time encrypt:"+encrypt;
        	    	assert(finalLen <= finalData.length) : "length is too long. encrypt: "+encrypt;

        	    	final int targetPos = Pipe.getWorkingBlobHeadPosition(output);
    	    		int blockLen = processBlock(finalLen, 0, finalData, targetPos, targetBuffer, targetMask);
    	    						
    	    		int finalLen2 = doFinalIntoRing(targetBuffer, targetPos+blockLen, targetMask);
    	    		    	    		
    	    		//must add the hash for final??
    	    		hashInputData(targetBuffer, targetPos+blockLen, targetMask, finalLen2);
    	       		int targetLen = blockLen + finalLen2;
		        
    	       		//////////
    	       		//must remove the header if we read part of it since it is not part of our data
    	    	    if (headerBytesToIgnore>0) {
    	    	    	targetLen -= headerBytesToIgnore;
    	    	    	
    	    	    	if (targetLen>=0) {
    	    	    		Pipe.copyBytesFromToRing(targetBuffer, targetPos+headerBytesToIgnore, targetMask, 
    	    	    								 targetBuffer, targetPos, targetMask, 
    	    	    								 targetLen);
    	    	    	}
    	    	    }
    	    		
    	    		if (targetLen>0) {
    	    			publishBockOut(targetPos, targetLen);
    	    		}
    	    		
    	    		isFinalRequested = false;
		        	resetToBeginning();
		        	
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
		
		//THIS BLOCK IS FOR DECRYPT ONLY.
		if ((fileOutputPosition<=0) && (!encrypt)) {
			
				//consume header block since it was just random data to protect the file
			    int avail = 0;
				if (Pipe.hasRoomForWrite(finalOutput)&&
					inputStream.hasRemainingBytes() &&
					(avail = inputStream.available()) >= guidBlockSize &&
					Pipe.hasRoomForWrite(output)) {
					
					fileOutputPosition = 0;
					
					Arrays.fill(rollingHash, (byte)0); //will be populated upon decrypt
					
					//////////////////////////////////////////////////
					//this call will grow filePosition and decrypt the first block
					final int targetPos = Pipe.getWorkingBlobHeadPosition(output);
					
					//System.out.println("in "+avail+" vs blob "+input.sizeOfBlobRing);
					
					int len = processBlock(avail,
							     		   inputStream.absolutePosition() & sourceMask,
							     		   sourceBuffer,
							     		   targetPos, targetBuffer, targetMask);
		
					//////////////////////////////////////
					//copy needed data from target into firstBlockBytesBacking array
					System.arraycopy(targetBuffer, targetPos, firstBlockBytesBacking, 0, guidBlockSize);
					inputStream.skip(avail);
					//////////////////////////////////////
					
					/////////////////////////////////////////////////
					rollingHashInputPos = guidBlockSize;
					/////////////////////////////////////////////////
								
					
					int rem = len-guidBlockSize;
					if (rem>0) {
						//must write this out
						System.arraycopy(targetBuffer, targetPos+guidBlockSize, 
										 targetBuffer, targetPos, 
								         rem);
						
						
						publishBockOut(targetPos, rem);
		
					}
					
					return true;
				} else {
					return false;
				}
				
			
		}
		
		if (Pipe.hasRoomForWrite(output) &&
			inputStream.hasRemainingBytes()) {
			//can not wrap off end so must limit
			final int rollLimit = sourceMask-(inputStream.absolutePosition() & sourceMask);
			assert(rollLimit>0);
			int avail = inputStream.available();
			
			if (avail<=0) {
				return false;//nothing to be done
			}
			
			if (avail <= rollLimit) {						
				
				int targetPos = Pipe.getWorkingBlobHeadPosition(output);
				
				int encSize = 0;
		
				if (encrypt && 0==fileOutputPosition) {
								
					//new random block
					long now = System.nanoTime();
					
					fileHeaderGenerator.setSeed(now);
					fileHeaderGenerator.nextBytes(firstBlockBytesBacking);

					long duration = System.nanoTime()-now;
					if (duration>1_000_000) {
						logger.info("warning: generating new random value took {} ns ",duration);
					}
					
					
					Arrays.fill(rollingHash, (byte)0); //will be populated upon encrypt
					rollingHashInputPos = guidBlockSize;
					
					
					//this is added here so it is part of the same write block
					//this call will NOT end up creating a final block due to it being first.
 					int len = processBlock(guidBlockSize, 0, firstBlockBytesBacking, 
 											targetPos, targetBuffer, targetMask);
 					
 					encSize += len;
 					targetPos += len;
				}				
				
				//debug what was encrypted
				//if (encrypt) {
				//	StringBuilder text = new StringBuilder("Encrypt: ");
				//	Appendables.appendUTF8(text, sourceBuffer, inputStream.absolutePosition(), avail, sourceMask);
				//	System.out.println(text.toString());
				//}
								
				//this call will create a final block since the GUID has already been added
				final int tSize = processBlock(avail, inputStream.absolutePosition() & sourceMask, sourceBuffer,
						                       targetPos, targetBuffer, targetMask); 
				encSize += tSize;
				
				//debug what was decrypted
				//if (!encrypt) {
				//	Appendables.appendUTF8(System.out, targetBuffer, targetPos, tSize, targetMask);	
				//	System.out.println();
				//}
	
				
				///////////////////////////////////////////////////
				//must publish regardless of size because we must maintain a 1 for 1 mapping of incoming
				//blocks to outgoing blocks, this actual data may go out with the next block but that is not
				//important because it will be saved as part of the stored final block
				///////////////////////////////////////////////////
				publishBockOut(Pipe.getWorkingBlobHeadPosition(output),encSize);					
			
				
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
	
	private int blockModCount = 0;

	private int processBlock(final int avail, int availPos, byte[] availBacking, 
			                 int targetPos, byte[] targetBacking, int targetMask) {
		assert(targetBacking.length>32);
		assert(avail>=0) : "avail out of range "+avail+" encrypt: "+encrypt;
		assert(avail<=availBacking.length);
		
		if (avail>0){
			
			assert(avail<=availBacking.length);
			if (encrypt) {
								
				hashInputData(availBacking, availPos, Integer.MAX_VALUE, avail);
				
			}
			
			try {
				
				    int requiredOutputRoom = c.getOutputSize(avail);
				    
				    int roomBeforeWrap = targetBacking.length - targetPos;
				    
				    int result = 0;
				    if (requiredOutputRoom <= roomBeforeWrap) {
						result = c.update(
												availBacking, 
												availPos, 
												avail, 
												targetBacking, 
												targetPos);
						
				    } else {
				    	System.out.println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
				    	System.out.println("tested when no room, if this worked remove this comment.");
				    	
				    	////////////////////////////////////////
				    	//this conditional is only needed because we may not have all the room
				    	//before the wrap and because c.update does not have the right signature to suppor this.
				    	/////////////////////////////////////
				    	if (requiredOutputRoom > tempWrapSpace.length) {
				    		tempWrapSpace = new byte[requiredOutputRoom*2];
				    	}
				    	
				    	System.out.println("avail "+avail+" pos "+availPos
				    			          +" target size "+tempWrapSpace.length
				    			          +"  req "+requiredOutputRoom);
				    	
				    	if (avail<100000) {
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
				    	
				    }
					
				    final long startPos = fileOutputPosition;				    
				    fileOutputPosition += result; //total count of all bytes processed
				    
					if (!encrypt) {
						hashInputData(targetBacking, targetPos, targetMask, result);	
					}				    
				    
				    //These bytes were sent to final so we must keep them for the next block
				    int extraBytesToReAdd = (blockModCount+avail)%passSize;
								
			        ////////////////////////////////////////////////
					//for encrypt we want to do final but only after we have written the first block bytes
			        //the guid must be esablished first or we will leak information about the tail
					if (encrypt && startPos >= guidBlockSize ) {
						

						blockModCount += extraBytesToReAdd;
						
						//NOTE: this final may be written before or after end of body.
						//      if after body will just use old end
						//      if before body will continue to use old end
						//      it is critical that we only have 1 outstanding at a time.
						//      so if we have an outstanding ack no new work should be taken.
						publishDoFinal();
								
						int srcPos = targetPos+result-passSize;
						if (srcPos<=0) {
							//use initial password if we are at the beginning of the file
							System.arraycopy(pass, 0, iv, 0, passSize);
						} else {
							//take last 16 and keep it for the new vi
							System.arraycopy(targetBuffer, srcPos, iv, 0, passSize);
						}
						
						ips = new IvParameterSpec(iv);						
						c.init(encrypt ? Cipher.ENCRYPT_MODE : Cipher.DECRYPT_MODE, sks, ips);
						
						
//							StringBuilder temp = new StringBuilder("** Missing bytes added: ");
//							Appendables.appendUTF8(temp, availBacking, 
//													(availPos+avail)-extraBytesToReAdd, 
//													extraBytesToReAdd, Integer.MAX_VALUE);
//							System.out.println(temp);
						
						
						int shouldBeZero = c.update(
													availBacking, 
													(availPos+avail)-extraBytesToReAdd, 
													extraBytesToReAdd, 
													tempWrapSpace, 
													0);
						assert(0 == shouldBeZero);
						
					} 			
					return result;
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		} else {
			return 0;
		}
	}

	private void hashInputData(byte[] availBacking, int availPos, int availMask, int avail) {
		for(int i = 0; i<avail; i++) {
			rollingHash[rollingHashMask & rollingHashInputPos++] ^= availBacking[availMask & (availPos+i)];
		}
	}

	private void publishDoFinal() throws IllegalBlockSizeException, ShortBufferException, BadPaddingException {

		assert(encrypt) : "never call for decrypt";

		PipeWriter.presumeWriteFragment(finalOutput, BlockStorageXmitSchema.MSG_WRITE_1);
		PipeWriter.writeLong(finalOutput,
							 BlockStorageXmitSchema.MSG_WRITE_1_FIELD_POSITION_12, 
							 (activeShuntPosition*(long)finalBlockSize));
		
		activeShuntPosition = 1&(activeShuntPosition+1);//toggle
		
		byte[] blob = Pipe.blob(finalOutput);						
		final int origPos = Pipe.getWorkingBlobHeadPosition(finalOutput);
	
		long lookupPosition = fileOutputPosition - passSize;
			
		//logger.trace("wrote lookup position of {} ",lookupPosition);
		DataOutputBlobWriter.write64(blob, finalOutput.blobMask, origPos, lookupPosition);//everything encoded so far.
		int lenPos = origPos+8;					
		
		int finalOffset = finalOutput.blobMask & (lenPos+4);
		int finalMask = finalOutput.blobMask;
		
		int finalLength = doFinalIntoRing(blob, finalOffset, finalMask);
		hashInputData(targetBuffer, finalOffset, targetMask, finalLength);
		
		//logger.trace("wrote final length of {} ",finalLength);
		DataOutputBlobWriter.write32(blob, finalOutput.blobMask, lenPos, finalLength);
				
		//we write full block to ensure file is filled up to that size.
		PipeWriter.writeSpecialBytesPosAndLen(finalOutput, 
				BlockStorageXmitSchema.MSG_WRITE_1_FIELD_PAYLOAD_11,
				finalBlockSize, origPos);
		
		
		lenPos += finalBlockSize;
		
		///////////////
		//added hash data to ensure we have no blank data
		//TODO: this can be used to ensure data is unmodified...
		//////////////
		
		int c = 5;
		long temp = lookupPosition - (safeOffset + (c*4)); //4 bytes per int
		while (--c>=0) {
			int hash = MurmurHash.hash32(rollingHash, (int)(temp & rollingHashMask), 4, 1337+c);
			DataOutputBlobWriter.write32(blob, finalOutput.blobMask, lenPos, hash);
			temp += 4;			
			lenPos += 4;
		}
		
	
    	int blockPos = origPos;
    	byte[] blockBack = finalOutput.blobRing;
    	int blockMask = finalOutput.blobMask;
    	    	
    	//////////////////////////////////////////////
    	//The filePosition is the last round number of bytes encrypted.
    	//In order to hash cleanly we must end at this point, eg use the previous data blocks
    	//to do this we will subtract the finalBlockSize from filePosition
    	//////////////////////////////////////////////    	
    	int rollingHashOffset = (int)((lookupPosition-safeOffset) & rollingHashMask);
 	    Pipe.xorBytesToBytes(rollingHash, rollingHashOffset, rollingHashMask, blockBack, blockPos, blockMask, finalBlockSize);

	    //Appendables.appendArray(Appendables.appendValue(System.out, offset), 
	    //		 				 rollingHash, offset, rollingHashMask, 64 ).append(" encrypt\n ");

		int consumed = PipeWriter.publishWrites(finalOutput);
		
		assert(consumed == finalBlockSize);

    	//logger.trace("final write encyprt: {}  position: {} ",encrypt, filePosition);
    	
		
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
