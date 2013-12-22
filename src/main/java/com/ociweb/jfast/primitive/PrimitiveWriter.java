package com.ociweb.jfast.primitive;

import com.ociweb.jfast.error.FASTException;





/**
 * PrimitiveWriter
 * 
 * Must be final and not implement any interface or be abstract.
 * In-lining the primitive methods of this class provides much
 * of the performance needed by this library.
 * 
 * 
 * @author Nathan Tippy
 *
 */

public final class PrimitiveWriter {

    //TODO: the write to output is not implemented right it must send one giant block when possible
	//TODO: we should have min and max block size? this may cover all cases.
	
    private static final int BLOCK_SIZE = 128;// in bytes
    private static final int BLOCK_SIZE_LAZY = (BLOCK_SIZE*3)+(BLOCK_SIZE>>1);
    private static final int POS_POS_SHIFT = 28;
    private static final int POS_POS_MASK = 0xFFFFFFF; //top 4 are bit pos, bottom 28 are byte pos
    
	private final FASTOutput output;
	final byte[] buffer;
		
	private final int minimizeLatency;
	private final long[] safetyStackPosPos;//low 28, location where the last byte was written to the pmap as bits are written
                                             //mid 04, working bit position 1-7 for next/last bit to be written.
											 //top 32, location (in skip list) where location of stopBytes+1 and end of max pmap length is found.
	
	private int safetyStackDepth;         //maximum depth of the stacks above
	private int position;
	private int limit;
	
	//both bytes but class def likes int much better for alignment
	private int pMapIdxWorking = 7;
	private int pMapByteAccum = 0;
		
	private long totalWritten;
	private final int[] flushSkips;//list of all skip nodes produced at the end of pmaps, may grow large with poor templates.
	private int flushSkipsIdxLimit; //where we add the new one, end of the list
	private int flushSkipsIdxPos;//next limit to use. as skips are consumed this pointer moves forward.
	
    private int nextBlockSize = -1;    //not bigger than BLOCK_SIZE
    private int nextBlockOffset = -1;  //position to begin copy data from
    private int pendingPosition = 0;   //new position after the read
    
    
	public PrimitiveWriter(FASTOutput output) {
		this(4096,output,128, false);
	}
	
	public PrimitiveWriter(int initBufferSize, FASTOutput output, int maxGroupCount, boolean minimizeLatency) {
		
		//TODO POS_POS_MASK can be shortened to only match the length of buffer.
		//but then buffer always must be a power of two.
		
		if (initBufferSize<BLOCK_SIZE_LAZY*2) {
			initBufferSize = BLOCK_SIZE_LAZY*2;
		}
		
		
		this.buffer = new byte[initBufferSize];
		this.position = 0;
		this.limit = 0;
		this.minimizeLatency = minimizeLatency? 1:0;
		this.safetyStackPosPos = new long[maxGroupCount];
				
		this.output = output;
		this.flushSkips = new int[maxGroupCount*2];//this may grow very large, to fields per group
		
		output.init(new DataTransfer(this));
	}
	

	
	public void reset() {
		this.position = 0;
		this.limit = 0;
		this.safetyStackDepth = 0;
		this.pMapIdxWorking = 7;
		this.pMapByteAccum = 0;
		this.flushSkipsIdxLimit = 0;
		this.flushSkipsIdxPos = 0;

		this.totalWritten = 0;
		
	}
	
    public long totalWritten() {
    	return totalWritten;
    }
 	

    
	public int nextBlockSize() {
		//return block size if the block is available
		if (nextBlockSize > 0 || position==limit) {
			assert(nextBlockSize!=0 || position==limit) : "nextBlockSize must be zero of position is at limit";
			return nextBlockSize;
		}
		//block was not available so build it
		//This check greatly helps None/Delta operations which do not use pmap.
		if (flushSkipsIdxPos==flushSkipsIdxLimit) {
			//all the data we have lines up with the end of the skip limit			
			//nothing to skip so flush the full block
			int avail = computeFlushToIndex() - (nextBlockOffset = position);
			pendingPosition = position+(nextBlockSize = (avail >= BLOCK_SIZE) ? BLOCK_SIZE : avail);
		} else {
			buildNextBlockWithSkips();
		}
		return nextBlockSize;
	}

	private void buildNextBlockWithSkips() {
			
		/*
		 * 		tempSkipPos = mergeSkips(tempSkipPos);
				int temp = flushSkipsIdxPos+1;
				if (temp<flushSkipsIdxLimit) {
								
					//TODO: move first block ? 
					//System.err.println("first block "+(tempSkipPos - position)+
					// " first skip "+( flushSkips[flushSkipsIdxPos+1]-tempSkipPos ));									
				}
		 */
		
		int sourceOffset = position;
		int targetOffset = position;
	    int reqLength = BLOCK_SIZE;	
	    
	    //do not change this value after this point we are committed to this location.
	    nextBlockOffset = targetOffset;
		
	    int localFlushSkipsIdxPos = flushSkipsIdxPos;
	    
		int sourceStop = flushSkips[localFlushSkipsIdxPos];
		
		//invariant for the loop
		final int localLastValid = computeLocalLastValid(flushSkipsIdxLimit);
		final int endOfData = limit;
		final int finalStop = targetOffset+reqLength;

		int flushRequest = sourceStop - sourceOffset;
		int targetStop = targetOffset+flushRequest;
		
		
		//this loop may have many more iterations than one might expect, ensure it only uses final and local values.
		//flush in parts that avoid the skip pos
		while (localFlushSkipsIdxPos < localLastValid && //stop if there are no more skip blocks
				sourceStop < endOfData &&   //stop at the end of the data
				targetStop <= finalStop     //stop at the end if the BLOCK
				) { 
			
			//keep accumulating
			if (targetOffset != sourceOffset) {		
				//System.err.println(sourceOffset+" "+targetOffset+" "+flushRequest+" "+buffer.length);
				System.arraycopy(buffer, sourceOffset, buffer, targetOffset, flushRequest);
			}
			sourceOffset = flushSkips[++localFlushSkipsIdxPos]; //new position in second part of flush skips
			sourceStop = flushSkips[++localFlushSkipsIdxPos]; //beginning of new skip.
			//stop becomes start so we can build a contiguous block
			targetOffset = targetStop;
			targetStop = targetOffset+(flushRequest = sourceStop - sourceOffset);

		} 
		
		reqLength = finalStop-targetOffset; //remaining bytes required
		flushSkipsIdxPos=localFlushSkipsIdxPos; //write back the local changes
		
		finishBuildingBlock(endOfData, sourceOffset, targetOffset, reqLength);
	}

	private void finishBuildingBlock(final int endOfData, int sourceOffset, int targetOffset, int reqLength) {
		int flushRequest = endOfData - sourceOffset;		
		if (flushRequest >= reqLength) {
			finishBlockAndLeaveRemaining(sourceOffset, targetOffset, reqLength);
		} else {
			//not enough data to fill full block, we are out of data to push.
			
			//reset to zero to save space if possible
			if (flushSkipsIdxPos==flushSkipsIdxLimit) {
				flushSkipsIdxPos=flushSkipsIdxLimit=0;
			} 		
			//keep accumulating
			if (sourceOffset!=targetOffset) {					
				System.arraycopy(buffer, sourceOffset, buffer, targetOffset, flushRequest);
			}
			nextBlockSize = BLOCK_SIZE - (reqLength-flushRequest);
			pendingPosition = sourceOffset+flushRequest;
		}
	}

	private void finishBlockAndLeaveRemaining(int sourceOffset, int targetOffset, int reqLength) {
		//more to flush than we need
		if (sourceOffset!=targetOffset) {
			System.arraycopy(buffer, sourceOffset, buffer, targetOffset, reqLength);
		}
		nextBlockSize = BLOCK_SIZE;
		pendingPosition = sourceOffset+reqLength;
	}

	private int computeLocalLastValid(int skipsIdxLimit) {
		int temp = flushSkips.length-2;
		return (skipsIdxLimit<temp)? skipsIdxLimit :temp;
	}

	public int nextOffset() {
	    if (nextBlockSize<=0) {
	    	throw new FASTException();
	    }
		int nextOffset = nextBlockOffset;
		
		totalWritten += nextBlockSize;
		
		position = pendingPosition;
		nextBlockSize = -1;
		nextBlockOffset = -1;
		pendingPosition = 0;
		
		if (0 == safetyStackDepth &&
			0 == flushSkipsIdxLimit &&
			position == limit) { 
			position = limit = 0;
		}	
		
		return nextOffset;
	}
	
	
    
    public final void flush () { //flush all 
    	output.flush();
    }
    
    protected int computeFlushToIndex() {
		if (safetyStackDepth>0) {
			//only need to check first entry on stack the rest are larger values
			int safetyLimit = (((int)safetyStackPosPos[0])&POS_POS_MASK) -1;//still modifying this position but previous is ready to go.
			return (safetyLimit < limit ? safetyLimit :limit);
		} else {
			return limit;
		}
	}


	//this requires the null adjusted length to be written first.
	public final void writeByteArrayData(byte[] data) {
		final int len = data.length;
		if (limit>buffer.length-len) {
			output.flush();
		}
		System.arraycopy(data, 0, buffer, limit, len);
		limit += len;
		
	}
		
	public final void writeNull() {
		if (limit>=buffer.length) {
			output.flush();
		}
		buffer[limit++] = (byte)0x80;
	}
	
	public final void writeLongSignedOptional(long value) {

		if (value >= 0) {
			writeLongSignedPos(value+1);
		} else {

			if ((value << 1) == 0) {
				if (limit > buffer.length - 10) {
					output.flush();
				}
				// encode the most negative possible number
				buffer[limit++] = (byte) (0x7F); // 8... .... .... ....
				buffer[limit++] = (byte) (0x00); // 7F.. .... .... ....
				buffer[limit++] = (byte) (0x00); // . FE .... .... ....
				buffer[limit++] = (byte) (0x00); // ...1 FC.. .... ....
				buffer[limit++] = (byte) (0x00); // .... .3F8 .... ....
				buffer[limit++] = (byte) (0x00); // .... ...7 F... ....
				buffer[limit++] = (byte) (0x00); // .... .... .FE. ....
				buffer[limit++] = (byte) (0x00); // .... .... ...1 FC..
				buffer[limit++] = (byte) (0x00); // .... .... .... 3F8.
				buffer[limit++] = (byte) (0x80); // .... .... .... ..7f
			} else {
				writeLongSignedNeg(value);
			}
		}
	}
	
	public final void writeLongSigned(long value) {

		if (value >= 0) {
			writeLongSignedPos(value);
		} else {

			if ((value << 1) == 0) {
				if (limit > buffer.length - 10) {
					output.flush();
				}
				// encode the most negative possible number
				buffer[limit++] = (byte) (0x7F); // 8... .... .... ....
				buffer[limit++] = (byte) (0x00); // 7F.. .... .... ....
				buffer[limit++] = (byte) (0x00); // . FE .... .... ....
				buffer[limit++] = (byte) (0x00); // ...1 FC.. .... ....
				buffer[limit++] = (byte) (0x00); // .... .3F8 .... ....
				buffer[limit++] = (byte) (0x00); // .... ...7 F... ....
				buffer[limit++] = (byte) (0x00); // .... .... .FE. ....
				buffer[limit++] = (byte) (0x00); // .... .... ...1 FC..
				buffer[limit++] = (byte) (0x00); // .... .... .... 3F8.
				buffer[limit++] = (byte) (0x80); // .... .... .... ..7f
			} else {
				writeLongSignedNeg(value);
			}
		}
	}

	private final void writeLongSignedNeg(long value) {
		// using absolute value avoids tricky word length issues
		long absv = -value;
		
		if (absv <= 0x0000000000000040l) {
			if (buffer.length - limit < 1) {
				output.flush();
			}
		} else {
			if (absv <= 0x0000000000002000l) {
				if (buffer.length - limit < 2) {
					output.flush();
				}
			} else {

				if (absv <= 0x0000000000100000l) {
					if (buffer.length - limit < 3) {
						output.flush();
					}
				} else {

					if (absv <= 0x0000000008000000l) {
						if (buffer.length - limit < 4) {
							output.flush();
						}
					} else {
						if (absv <= 0x0000000400000000l) {
					
							if (buffer.length - limit < 5) {
								output.flush();
							}
						} else {
							writeLongSignedNegSlow(absv, value);
							return;
						}
						buffer[limit++] = (byte)(((value >> 28) & 0x7F));
					}
					buffer[limit++] = (byte) (((value >> 21) & 0x7F));
				}
				buffer[limit++] = (byte) (((value >> 14) & 0x7F));
			}
			buffer[limit++] = (byte) (((value >> 7) & 0x7F));
		}
		buffer[limit++] = (byte) (((value & 0x7F) | 0x80));

	}

	private final void writeLongSignedNegSlow(long absv, long value) {
		if (absv <= 0x0000020000000000l) {
			if (buffer.length - limit < 6) {
				output.flush();
			}
		} else {
			writeLongSignedNegSlow2(absv, value);
		}

		// used by all
		buffer[limit++] = (byte) (((value >> 35) & 0x7F));
		buffer[limit++] = (byte) (((value >> 28) & 0x7F));
		buffer[limit++] = (byte) (((value >> 21) & 0x7F));
		buffer[limit++] = (byte) (((value >> 14) & 0x7F));
		buffer[limit++] = (byte) (((value >> 7) & 0x7F));
		buffer[limit++] = (byte) (((value & 0x7F) | 0x80));
	}

	private void writeLongSignedNegSlow2(long absv, long value) {
		if (absv <= 0x0001000000000000l) {
			if (buffer.length - limit < 7) {
				output.flush();
			}
		} else {
			if (absv <= 0x0080000000000000l) {
				if (buffer.length - limit < 8) {
					output.flush();
				}
			} else {
				if (buffer.length - limit < 9) {
					output.flush();
				}
				buffer[limit++] = (byte) (((value >> 56) & 0x7F));
			}
			buffer[limit++] = (byte) (((value >> 49) & 0x7F));
		}
		buffer[limit++] = (byte) (((value >> 42) & 0x7F));
	}

	private final void writeLongSignedPos(long value) {
				
		if (value < 0x0000000000000040l) {
			if (buffer.length - limit < 1) {
				output.flush();
			}
		} else {
			if (value < 0x0000000000002000l) {
				if (buffer.length - limit < 2) {
					output.flush();
				}
			} else {

				if (value < 0x0000000000100000l) {
					if (buffer.length - limit < 3) {
						output.flush();
					}
				} else {

					if (value < 0x0000000008000000l) {
						if (buffer.length - limit < 4) {
							output.flush();
						}
					} else {
						if (value < 0x0000000400000000l) {
					
							if (buffer.length - limit < 5) {
								output.flush();
							}
						} else {
							writeLongSignedPosSlow(value);
							return;
						}
						buffer[limit++] = (byte)(((value >> 28) & 0x7F));
					}
					buffer[limit++] = (byte) (((value >> 21) & 0x7F));
				}
				buffer[limit++] = (byte) (((value >> 14) & 0x7F));
			}
			buffer[limit++] = (byte) (((value >> 7) & 0x7F));
		}
		buffer[limit++] = (byte) (((value & 0x7F) | 0x80));
	}

	private final void writeLongSignedPosSlow(long value) {
		if (value < 0x0000020000000000l) {
			if (buffer.length - limit < 6) {
				output.flush();
			}
		} else {
			if (value < 0x0001000000000000l) {
				if (buffer.length - limit < 7) {
					output.flush();
				}
			} else {
				if (value < 0x0080000000000000l) {
					if (buffer.length - limit < 8) {
						output.flush();
					}
				} else {
					if (value < 0x4000000000000000l) {
						if (buffer.length - limit < 9) {
							output.flush();
						}
					} else {
						if (buffer.length - limit < 10) {
							output.flush();
						}
						buffer[limit++] = (byte) (((value >> 63) & 0x7F));
					}
					buffer[limit++] = (byte) (((value >> 56) & 0x7F));
				}
				buffer[limit++] = (byte) (((value >> 49) & 0x7F));
			}
			buffer[limit++] = (byte) (((value >> 42) & 0x7F));
		}

		// used by all
		buffer[limit++] = (byte) (((value >> 35) & 0x7F));
		buffer[limit++] = (byte) (((value >> 28) & 0x7F));
		buffer[limit++] = (byte) (((value >> 21) & 0x7F));
		buffer[limit++] = (byte) (((value >> 14) & 0x7F));
		buffer[limit++] = (byte) (((value >> 7) & 0x7F));
		buffer[limit++] = (byte) (((value & 0x7F) | 0x80));
	}
	
	public final void writeLongUnsigned(long value) {

			if (value < 0x0000000000000080l) {
				if (buffer.length - limit < 1) {
					output.flush();
				}
			} else {
				if (value < 0x0000000000004000l) {
					if (buffer.length - limit < 2) {
						output.flush();
					}
				} else {

					if (value < 0x0000000000200000l) {
						if (buffer.length - limit < 3) {
							output.flush();
						}
					} else {

						if (value < 0x0000000010000000l) {
							if (buffer.length - limit < 4) {
								output.flush();
							}
						} else {
							if (value < 0x0000000800000000l) {
						
								if (buffer.length - limit < 5) {
									output.flush();
								}
							} else {
								writeLongUnsignedSlow(value);
								return;
							}
							buffer[limit++] = (byte)(((value >> 28) & 0x7F));
						}
						buffer[limit++] = (byte) (((value >> 21) & 0x7F));
					}
					buffer[limit++] = (byte) (((value >> 14) & 0x7F));
				}
				buffer[limit++] = (byte) (((value >> 7) & 0x7F));
			}
			buffer[limit++] = (byte) (((value & 0x7F) | 0x80));
	}

	private final void writeLongUnsignedSlow(long value) {
		if (value < 0x0000040000000000l) {
			if (buffer.length - limit < 6) {
				output.flush();
			}
		} else {
			writeLongUnsignedSlow2(value);
		}

		// used by all
		buffer[limit++] = (byte) (((value >> 35) & 0x7F));
		buffer[limit++] = (byte) (((value >> 28) & 0x7F));
		buffer[limit++] = (byte) (((value >> 21) & 0x7F));
		buffer[limit++] = (byte) (((value >> 14) & 0x7F));
		buffer[limit++] = (byte) (((value >> 7) & 0x7F));
		buffer[limit++] = (byte) (((value & 0x7F) | 0x80));

	}

	private void writeLongUnsignedSlow2(long value) {
		if (value < 0x0002000000000000l) {
			if (buffer.length - limit < 7) {
				output.flush();
			}
		} else {
			if (value < 0x0100000000000000l) {
				if (buffer.length - limit < 8) {
					output.flush();
				}
			} else {
				if (value < 0x8000000000000000l) {
					if (buffer.length - limit < 9) {
						output.flush();
					}
				} else {
					if (buffer.length - limit < 10) {
						output.flush();
					}
					buffer[limit++] = (byte) (((value >> 63) & 0x7F));
				}
				buffer[limit++] = (byte) (((value >> 56) & 0x7F));
			}
			buffer[limit++] = (byte) (((value >> 49) & 0x7F));
		}
		buffer[limit++] = (byte) (((value >> 42) & 0x7F));
	}
	
	
	public final void writeIntegerSignedOptional(int value) {
		if (value >= 0) { 
			writeIntegerSignedPos(value+1);
		} else {
			if ((value << 1) == 0) {
				if (limit > buffer.length - 5) {
					output.flush();
				}
				// encode the most negative possible number
				buffer[limit++] = (byte) (0x7F); // .... ...7 F... ....
				buffer[limit++] = (byte) (0x00); // .... .... .FE. ....
				buffer[limit++] = (byte) (0x00); // .... .... ...1 FC..
				buffer[limit++] = (byte) (0x00); // .... .... .... 3F8.
				buffer[limit++] = (byte) (0x80); // .... .... .... ..7f
			} else {
				writeIntegerSignedNeg(value);
			}
		}
	}
	
	public final void writeIntegerSigned(int value) {
		if (value >= 0) { 
			writeIntegerSignedPos(value);
		} else {
			if ((value << 1) == 0) {
				if (limit > buffer.length - 5) {
					output.flush();
				}
				// encode the most negative possible number
				buffer[limit++] = (byte) (0x7F); // .... ...7 F... ....
				buffer[limit++] = (byte) (0x00); // .... .... .FE. ....
				buffer[limit++] = (byte) (0x00); // .... .... ...1 FC..
				buffer[limit++] = (byte) (0x00); // .... .... .... 3F8.
				buffer[limit++] = (byte) (0x80); // .... .... .... ..7f
			} else {
				writeIntegerSignedNeg(value);
			}
		}
	}

	private void writeIntegerSignedNeg(int value) {
	    // using absolute value avoids tricky word length issues
	    int absv = -value;
	    
	    
		if (absv <= 0x00000040) {
			if (buffer.length - limit < 1) {
				output.flush();
			}
		} else {
			if (absv <= 0x00002000) {
				if (buffer.length - limit < 2) {
					output.flush();
				}
			} else {
				if (absv <= 0x00100000) {
					if (buffer.length - limit < 3) {
						output.flush();
					}
				} else {
					if (absv <= 0x08000000) {
						if (buffer.length - limit < 4) {
							output.flush();
						}
					} else {
						if (buffer.length - limit < 5) {
							output.flush();
						}
						buffer[limit++] = (byte)(((value >> 28) & 0x7F));
					}
					buffer[limit++] = (byte) (((value >> 21) & 0x7F));
				}
				buffer[limit++] = (byte) (((value >> 14) & 0x7F));
			}
			buffer[limit++] = (byte) (((value >> 7) & 0x7F));
		}
		buffer[limit++] = (byte) (((value & 0x7F) | 0x80));
	    
	    
	}

	//TODO: Kryo is 2x faster than this.
	// 1. in the test it is writing directly to ByteBuffer with no internal buffer.
	// 2. it copies one int at a time into byte buffer.
	
	private void writeIntegerSignedPos(int value) {
		
		if (value < 0x00000040) {
			if (buffer.length - limit < 1) {
				output.flush();
			}
		} else {
			if (value < 0x00002000) {
				if (buffer.length - limit < 2) {
					output.flush();
				}
			} else {
				if (value < 0x00100000) {
					if (buffer.length - limit < 3) {
						output.flush();
					}
				} else {
					if (value < 0x08000000) {
						if (buffer.length - limit < 4) {
							output.flush();
						}
					} else {
						if (buffer.length - limit < 5) {
							output.flush();
						}
						buffer[limit++] = (byte)(((value >> 28) & 0x7F));
					}
					buffer[limit++] = (byte) (((value >> 21) & 0x7F));
				}
				buffer[limit++] = (byte) (((value >> 14) & 0x7F));
			}
			buffer[limit++] = (byte) (((value >> 7) & 0x7F));
		}
		buffer[limit++] = (byte) (((value & 0x7F) | 0x80));
				
	}
	
	public final void writeIntegerUnsigned(int value) {
		
		if (value < 0x00000080) {
			if (buffer.length - limit < 1) {
				output.flush();
			}
		} else {
			if (value < 0x00004000) {
				if (buffer.length - limit < 2) {
					output.flush();
				}
			} else {
				if (value < 0x00200000) {
					if (buffer.length - limit < 3) {
						output.flush();
					}
				} else {
					if (value < 0x10000000) {
						if (buffer.length - limit < 4) {
							output.flush();
						}
					} else {
						if (buffer.length - limit < 5) {
							output.flush();
						}
						buffer[limit++] = (byte)(((value >> 28) & 0x7F));
					}
					buffer[limit++] = (byte) (((value >> 21) & 0x7F));
				}
				buffer[limit++] = (byte) (((value >> 14) & 0x7F));
			}
			buffer[limit++] = (byte) (((value >> 7) & 0x7F));
		}
		buffer[limit++] = (byte) (((value & 0x7F) | 0x80));
	}


	///////////////////////////////////
	//New PMAP writer implementation
	///////////////////////////////////
	
	//called only at the beginning of a group.
	public final void openPMap(int maxBytes) {
		
		assert(maxBytes>0) : "Do not call openPMap if it is not expected to be used.";
		
		if (limit > buffer.length - maxBytes) {
			output.flush();
		}
		
		//save the current partial byte.
		//always save because pop will always load
		if (safetyStackDepth>0) {			
			int s = safetyStackDepth-1;
			assert(s>=0) : "Must call pushPMap(maxBytes) before attempting to write bits to it";		

			//NOTE: can inc pos pos because it will not overflow.
			long stackFrame = safetyStackPosPos[s];
			int idx = (int)stackFrame & POS_POS_MASK;
			if (0 != (buffer[idx] = (byte)pMapByteAccum)) {	
				//set the last known non zero bit so we can avoid scanning for it.
				flushSkips[(int)(stackFrame>>32)] = idx;
			}							
			safetyStackPosPos[s] = (((long)pMapIdxWorking)<<POS_POS_SHIFT) | idx;

		} 		
		//NOTE: pos pos, new position storage so top bits are always unset and no need to set.

		safetyStackPosPos[safetyStackDepth] =   ( ((long)flushSkipsIdxLimit) <<32) | limit;

		safetyStackDepth++;
		flushSkips[flushSkipsIdxLimit++] = limit+1;//default minimum size for present PMap
		flushSkips[flushSkipsIdxLimit++] = (limit += maxBytes);//this will remain as the fixed limit					
		
		//reset so we can start accumulating bits in the new pmap.
		pMapIdxWorking = 7;
		pMapByteAccum = 0;			
		
	}

	
	//called only at the end of a group.
	public final void closePMap() {
		/////
		//the PMap is ready for writing.
		//bit writes will go to previous bitmap location
		/////
		//push open writes
	   	int s = --safetyStackDepth;
		assert(s>=0) : "Must call pushPMap(maxBytes) before attempting to write bits to it";
						
		//final byte to be saved into the feed. //NOTE: pos pos can inc because it does not roll over.

		if (0 != (buffer[(int)(POS_POS_MASK&safetyStackPosPos[s]++)] = (byte)pMapByteAccum)) {	
			//close is too late to discover overflow so it is NOT done here.
			//
			long stackFrame = safetyStackPosPos[s];
			//set the last known non zero bit so we can avoid scanning for it. 
			buffer[(flushSkips[(int)(stackFrame>>32)] = (int)(stackFrame&POS_POS_MASK))-1] |= 0x80;
		} else {
			//must set stop bit now that we know where pmap stops.
			buffer[ flushSkips[(int)(safetyStackPosPos[s]>>32)]          -1] |= 0x80;
		}
				
		//restore the old working bits if there is a previous pmap.
		if (safetyStackDepth>0) {	
		
			//NOTE: pos pos will not roll under so we can just subtract
			long posPos = safetyStackPosPos[safetyStackDepth-1];
			pMapByteAccum = buffer[(int)(posPos&POS_POS_MASK)];
			pMapIdxWorking = (byte)(0xF&(posPos>>POS_POS_SHIFT));

		}
		
		//ensure low-latency for groups, or 
    	//if we can reset the safety stack and we have one block ready go ahead and flush
    	if (minimizeLatency!=0 || (0==safetyStackDepth && (limit-position)>(BLOCK_SIZE_LAZY) )) { //one block and a bit left over so we need bigger.
    		output.flush();
    	}
    	
	}

	//called by ever field that needs to set a bit either 1 or 0
	//must be fast because it is frequently called.
	public final void writePMapBit(byte bit) {
		if (0 == --pMapIdxWorking) {
			assert(safetyStackDepth-1>=0) : "Must call pushPMap(maxBytes) before attempting to write bits to it";
					
			//save this byte and if it was not a zero save that fact as well //NOTE: pos pos will not rollover so can inc
			if (0 != (buffer[(int)(POS_POS_MASK&safetyStackPosPos[safetyStackDepth-1]++)] = (byte) (pMapByteAccum | bit))) {	
				long stackFrame = safetyStackPosPos[safetyStackDepth-1];
				//set the last known non zero bit so we can avoid scanning for it. 
				int lastPopulatedIdx = (int)(POS_POS_MASK&stackFrame);// one has been added for exclusive use of range
				//writing the pmap bit is the ideal place to detect overflow of the bits based on expectations.
				assert (lastPopulatedIdx<flushSkips[(int)(stackFrame>>32)+1]):"Too many bits in PMAP.";
				flushSkips[(int)(stackFrame>>32)] = lastPopulatedIdx;
			}	
			
			pMapIdxWorking = 7;
			pMapByteAccum = 0;
			
		} else {
			pMapByteAccum |= (bit<<pMapIdxWorking);
		}
	}

	public boolean isPMapOpen() {
		return safetyStackDepth>0;
	}
	
	public final void writeTextASCII(CharSequence value) {
		
		int length = value.length();
		if (0==length) {
			encodeZeroLengthASCII();
			return;
		} else	if (limit>buffer.length-length) {
			//if it was not zero and was too long flush
			output.flush();
		}
		int c = 0;
		while (--length>0) {
			buffer[limit++] = (byte)value.charAt(c++);
		}
		buffer[limit++] = (byte)(0x80|value.charAt(c));
		
	}
	
	public final void writeTextASCIIAfter(int start, CharSequence value) {
		
		int length = value.length()-start;
		if (0==length) {
			encodeZeroLengthASCII();
			return;
		} else	if (limit>buffer.length-length) {
			//if it was not zero and was too long flush
			output.flush();
		}
		int c = start;
		while (--length>0) {
			buffer[limit++] = (byte)value.charAt(c++);
		}
		buffer[limit++] = (byte)(0x80|value.charAt(c));
		
	}

	public final void writeTextASCIIBefore(CharSequence value, int stop) {
		
		int length = stop;
		if (0==length) {
			encodeZeroLengthASCII();
			return;
		} else	if (limit>buffer.length-length) {
			//if it was not zero and was too long flush
			output.flush();
		}
		int c = 0;
		while (--length>0) {
			buffer[limit++] = (byte)value.charAt(c++);
		}
		buffer[limit++] = (byte)(0x80|value.charAt(c));
		
	}
	
	private void encodeZeroLengthASCII() {
		if (limit>buffer.length-2) {
			output.flush();
		}
		buffer[limit++] = (byte)0;
		buffer[limit++] = (byte)0x80;
	}

	public void writeTextASCII(char[] value, int offset, int length) {

		if (0==length) {
			encodeZeroLengthASCII();
			return;
		} else	if (limit>buffer.length-length) {
			//if it was not zero and was too long flush
			output.flush();
		}
		while (--length>0) {
			buffer[limit++] = (byte)value[offset++];
		}
		buffer[limit++] = (byte)(0x80|value[offset]);
	}
	
	public void writeTextUTF(CharSequence value) {
		int len = value.length();
		int c = 0;
		while (c<len) {
			encodeSingleChar(value.charAt(c++));
		}
	}
	
	public void writeTextUTFBefore(CharSequence value, int stop) {
		int len = stop;
		int c = 0;
		while (c<len) {
			encodeSingleChar(value.charAt(c++));
		}		
	}
	
	
	public void writeTextUTFAfter(int start, CharSequence value) {
		int len = value.length();
		int c = start;
		while (c<len) {
			encodeSingleChar(value.charAt(c++));
		}		
	}

	public void writeTextUTF(char[] value, int offset, int length) {
		while (--length>=0) {
			encodeSingleChar(value[offset++]);
		}
	}

	private void encodeSingleChar(int c) {
		
		if (c<=0x007F) {
			//code point 7
			if (limit>buffer.length-1) {
				output.flush();
			}
			buffer[limit++] = (byte)c;
		} else {
			if (c<=0x07FF) {
				//code point 11
				if (limit>buffer.length-2) {
					output.flush();
				}
				buffer[limit++] = (byte)(0xC0|((c>>6)&0x1F));
			} else {
				if (c<=0xFFFF) {
					//code point 16
					if (limit>buffer.length-3) {
						output.flush();
					}
					buffer[limit++] = (byte)(0xE0|((c>>12)&0x0F));
				} else {
					encodeSingleCharSlow(c);
				}
				buffer[limit++] = (byte)(0x80 |((c>>6) &0x3F));
			}
			buffer[limit++] = (byte)(0x80 |((c)   &0x3F));
		}
	}

	protected void encodeSingleCharSlow(int c) {
		if (c<0x1FFFFF) {
			//code point 21
			if (limit>buffer.length-4) {
				output.flush();
			}
			buffer[limit++] = (byte)(0xF0|((c>>18)&0x07));
		} else {
			if (c<0x3FFFFFF) {
				//code point 26
				if (limit>buffer.length-5) {
					output.flush();
				}
				buffer[limit++] = (byte)(0xF8|((c>>24)&0x03));
			} else {
				if (c<0x7FFFFFFF) {
					//code point 31
					if (limit>buffer.length-6) {
						output.flush();
					}
					buffer[limit++] = (byte)(0xFC|((c>>30)&0x01));
				} else {
					throw new UnsupportedOperationException("can not encode char with value: "+c);
				}
				buffer[limit++] = (byte)(0x80 |((c>>24) &0x3F));
			}
			buffer[limit++] = (byte)(0x80 |((c>>18) &0x3F));
		}						
		buffer[limit++] = (byte)(0x80 |((c>>12) &0x3F));
	}
	
}
