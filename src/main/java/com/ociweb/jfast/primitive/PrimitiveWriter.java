package com.ociweb.jfast.primitive;

import com.ociweb.jfast.read.FASTException;





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

	private final FASTOutput output;
	
	/*
	 * TODO: New flush design
	 * Fixed export block size 64Bytes - is optimal for cache lines and memory layout (mechanical sympathy)
	 * 
	 * upon flush must gather 64 bytes of data together and send to FASTOutput for write.
	 * 
	 * DataTransfer.length() <return 0-64>
	 * DataTransfer.offset() <use with array passed in init>
	 * 
	 *  research JVM byte alignment. should copy start at boundaries?
	 *  research System.arrayCopy speed.
	 *  
	 *  Add constant for NO_FIELD_ID and test against kryo.
	 * 
	 * 
	 * 
	 */
	
	final byte[] buffer;
	private final int bufferLength;
	
	private int position;
	private int limit;
	
	private final int[] safetyStackPosition; //location where the last byte was written to the pmap as bits are written
	private final int[] safetyStackFlushIdx; //location (in skip list) where location of stopBytes+1 and end of max pmap length is found.
	private final byte[] safetyStackTemp; //working bit position 1-7 for next/last bit to be written.
	private int   safetyStackDepth;       //maximum depth of the stacks above
	byte pMapIdxWorking = 7;
	byte pMapByteAccum = 0;
	
	private final int[] flushSkips;//list of all skip nodes produced at the end of pmaps, may grow large with poor templates.
	private final int lastValid;
	
	private int   flushSkipsIdxLimit; //where we add the new one, end of the list
	private int   flushSkipsIdxPos;//next limit to use. as skips are consumed this pointer moves forward.

	private long totalWritten;
	
	private final boolean minimizeLatency;
	
	public PrimitiveWriter(FASTOutput output) {
		this(4096,output,128, false);
	}
	
	public PrimitiveWriter(int initBufferSize, FASTOutput output, int maxGroupCount, boolean minimizeLatency) {
		
		this.buffer = new byte[initBufferSize];
		this.bufferLength = buffer.length;
		this.position = 0;
		this.limit = 0;
		this.minimizeLatency = minimizeLatency;
		//NOTE: may be able to optimize this so these 3 are shorter
		safetyStackPosition = new int[maxGroupCount];
		safetyStackFlushIdx = new int[maxGroupCount];
		safetyStackTemp = new byte[maxGroupCount];
		//max total groups
		flushSkips = new int[maxGroupCount*2];//this may grow very large, to fields per group
		lastValid = flushSkips.length-2;
		
		this.output = output;
		
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
 	
    //TODO: there is some write bug that becomes obvious by changing this value.
    static final int BLOCK_SIZE = 256;// in bytes
    
    int nextBlockSize = -1;
    int nextBlockOffset = -1; 
    int pendingPosition = 0;
    
	public int nextBlockSize() {
		//return block size if the block is available
		if (nextBlockSize > 0) {
			return nextBlockSize;
		}
		//block was not available so build it
		int flushTo = computeFlushToIndex();		
		
		int toFlush = flushTo-position;
		if (0==toFlush) {
			nextBlockSize = 0;
			nextBlockOffset = -1; 
			//no need to do any work.
			return 0;
		} else if (flushSkipsIdxPos==flushSkipsIdxLimit) {
			
			//all the data we have lines up with the end of the skip limit			
			//nothing to skip so flush the full block
			
			nextBlockOffset = position;
			
			int avail = flushTo - position;
			if (avail >= BLOCK_SIZE) {
				nextBlockSize = BLOCK_SIZE;
				pendingPosition = position+BLOCK_SIZE;
			} else { 
				//not enough to fill a block so we must flush what we have.
				nextBlockSize = avail;
				pendingPosition = position+avail;
			}
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
		
		int flushTo = limit;
		int copyFromOffset = position;
		int copyToOffset = position;
	    int reqLength = BLOCK_SIZE;	
		
		int tempSkipPos = flushSkips[flushSkipsIdxPos];
				
		int localLastValid = computeLocalLastValid(flushSkipsIdxLimit);
		
		//flush in parts that avoid the skip pos
		while (flushSkipsIdxPos < localLastValid && //we still have skips
				tempSkipPos < flushTo                  //skip stops before goal
				) { 
			
			tempSkipPos = mergeSkips(tempSkipPos);
			
			int flushRequest = tempSkipPos - copyFromOffset;
			
			if (flushRequest >= reqLength) {
				finishBlockAndLeaveRemaining(copyFromOffset, copyToOffset, reqLength);
				return;
			} else {
				//keep accumulating
				if (copyToOffset != copyFromOffset) {			
						System.arraycopy(buffer, copyFromOffset, buffer, copyToOffset, flushRequest);
				}
				copyToOffset += flushRequest;
				reqLength -= flushRequest;

			}						
			
			//did flush up to skip so set rollingPos to after skip
			copyFromOffset = flushSkips[++flushSkipsIdxPos]; //new position in second part of flush skips
			tempSkipPos = flushSkips[++flushSkipsIdxPos]; //beginning of new skip.

		} 

		//reset to zero to save space if possible
		if (flushSkipsIdxPos==flushSkipsIdxLimit) {
			flushSkipsIdxPos=flushSkipsIdxLimit=0;
			
			int flushRequest = flushTo - copyFromOffset;
			
			if (flushRequest >= reqLength) {
				finishBlockAndLeaveRemaining(copyFromOffset, copyToOffset, reqLength);
				return;
			} else {
				//keep accumulating
				if (copyFromOffset!=copyToOffset) {					
					System.arraycopy(buffer, copyFromOffset, buffer, copyToOffset, flushRequest);
				}
				copyToOffset += flushRequest;
				reqLength -= flushRequest;
				copyFromOffset += flushRequest;
			}
		} 
		
		
		nextBlockOffset = 0;
		nextBlockSize = BLOCK_SIZE - reqLength;
		pendingPosition = copyFromOffset;
				
	}

	private int mergeSkips(int tempSkipPos) {
		//if the skip is zero bytes just flush it all together
		int temp = flushSkipsIdxPos+1;
		
		if (temp<flushSkipsIdxLimit && flushSkips[temp]==tempSkipPos) {
				++flushSkipsIdxPos;
				tempSkipPos = flushSkips[++flushSkipsIdxPos];
		}
		return tempSkipPos;
	}

	private void finishBlockAndLeaveRemaining(int rollingPos, int x, int reqLength) {
		//more to flush than we need
		if (rollingPos!=x) {
			System.arraycopy(buffer, rollingPos, buffer, x, reqLength);
		}
		nextBlockOffset = 0;
		nextBlockSize = BLOCK_SIZE;
		pendingPosition = rollingPos+reqLength;
	}

	private int computeLocalLastValid(int skipsIdxLimit) {
		int localLastValid;
		if (skipsIdxLimit<lastValid) {
			localLastValid = skipsIdxLimit;
		} else {
			localLastValid = lastValid;
		}
		return localLastValid;
	}

	public int nextOffset() {
	    if (nextBlockSize<0) {
	    	throw new FASTException();
	    }
		int nextOffset = nextBlockOffset;
		
		totalWritten += nextBlockSize;
		
		position = pendingPosition;
		nextBlockSize = -1;
		nextBlockOffset = -1;
		pendingPosition = 0;
		
		if (position == limit &&
		    0 == safetyStackDepth &&
			0 == flushSkipsIdxLimit) { 
			position = limit = 0;
		}	
		
		return nextOffset;
	}
	
	
    
    public final void flush () { //flush all 
    	output.flush();
    }
    
    protected int computeFlushToIndex() {
		int flushTo = limit;
		if (safetyStackDepth>0) {
			//only need to check first entry on stack the rest are larger values
			int safetyLimit = safetyStackPosition[0]-1;//still modifying this position but previous is ready to go.
			if (safetyLimit < flushTo) {
				flushTo = safetyLimit;
			}		
		}
		return flushTo;
	}


	//this requires the null adjusted length to be written first.
	public final void writeByteArrayData(byte[] data) {
		final int len = data.length;
		if (limit>bufferLength-len) {
			output.flush();
		}
		System.arraycopy(data, 0, buffer, limit, len);
		limit += len;
		
	}
	
	//TODO: if it is myCharSeq or byteSeq build custom writer to avoid value.charAt() repeated call.
    //       this will be helpful in copy, tail, delta operations.
	
	public final void writeASCII(CharSequence value) {
		
		int length = value.length();
		if (0==length) {
			if (limit>bufferLength-2) {
				output.flush();
			}
			buffer[limit++] = (byte)0;
			buffer[limit++] = (byte)0x80;
			return;
		}
		if (limit>bufferLength-length) {
			output.flush();
		}
		int c = 0;
		byte[] b = buffer;
		int lim = limit;
		while (--length>0) {
			b[lim++] = (byte)value.charAt(c++);
		}
		b[lim++] = (byte)(0x80|value.charAt(c));
		limit = lim;
		
	}
	
	public final void writeNull() {
		if (limit>=bufferLength) {
			output.flush();
		}
		buffer[limit++] = (byte)0x80;
	}
	
	public final void writeSignedLongNullable(long value) {

		if (value >= 0) {
			writeSignedLongPos(value+1);
		} else {

			if ((value << 1) == 0) {
				if (limit > bufferLength - 10) {
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
				writeSignedLongNeg(value);
			}
		}
	}
	
	public final void writeSignedLong(long value) {

		if (value >= 0) {
			writeSignedLongPos(value);
		} else {

			if ((value << 1) == 0) {
				if (limit > bufferLength - 10) {
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
				writeSignedLongNeg(value);
			}
		}
	}

	private final void writeSignedLongNeg(long value) {
		// using absolute value avoids tricky word length issues
		long absv = -value;
		
		if (absv <= 0x0000000000000040l) {
			if (bufferLength - limit < 1) {
				output.flush();
			}
		} else {
			if (absv <= 0x0000000000002000l) {
				if (bufferLength - limit < 2) {
					output.flush();
				}
			} else {

				if (absv <= 0x0000000000100000l) {
					if (bufferLength - limit < 3) {
						output.flush();
					}
				} else {

					if (absv <= 0x0000000008000000l) {
						if (bufferLength - limit < 4) {
							output.flush();
						}
					} else {
						if (absv <= 0x0000000400000000l) {
					
							if (bufferLength - limit < 5) {
								output.flush();
							}
						} else {
							writeSignedLongNegSlow(absv, value);
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

	private final void writeSignedLongNegSlow(long absv, long value) {
		if (absv <= 0x0000020000000000l) {
			if (bufferLength - limit < 6) {
				output.flush();
			}
		} else {
			writeSignedLongNegSlow2(absv, value);
		}

		// used by all
		buffer[limit++] = (byte) (((value >> 35) & 0x7F));
		buffer[limit++] = (byte) (((value >> 28) & 0x7F));
		buffer[limit++] = (byte) (((value >> 21) & 0x7F));
		buffer[limit++] = (byte) (((value >> 14) & 0x7F));
		buffer[limit++] = (byte) (((value >> 7) & 0x7F));
		buffer[limit++] = (byte) (((value & 0x7F) | 0x80));
	}

	private void writeSignedLongNegSlow2(long absv, long value) {
		if (absv <= 0x0001000000000000l) {
			if (bufferLength - limit < 7) {
				output.flush();
			}
		} else {
			if (absv <= 0x0080000000000000l) {
				if (bufferLength - limit < 8) {
					output.flush();
				}
			} else {
				if (bufferLength - limit < 9) {
					output.flush();
				}
				buffer[limit++] = (byte) (((value >> 56) & 0x7F));
			}
			buffer[limit++] = (byte) (((value >> 49) & 0x7F));
		}
		buffer[limit++] = (byte) (((value >> 42) & 0x7F));
	}

	private final void writeSignedLongPos(long value) {
		
		if (value < 0x0000000000000040l) {
			if (bufferLength - limit < 1) {
				output.flush();
			}
		} else {
			if (value < 0x0000000000002000l) {
				if (bufferLength - limit < 2) {
					output.flush();
				}
			} else {

				if (value < 0x0000000000100000l) {
					if (bufferLength - limit < 3) {
						output.flush();
					}
				} else {

					if (value < 0x0000000008000000l) {
						if (bufferLength - limit < 4) {
							output.flush();
						}
					} else {
						if (value < 0x0000000400000000l) {
					
							if (bufferLength - limit < 5) {
								output.flush();
							}
						} else {
							writeSignedLongPosSlow(value);
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

	private final void writeSignedLongPosSlow(long value) {
		if (value < 0x0000020000000000l) {
			if (bufferLength - limit < 6) {
				output.flush();
			}
		} else {
			if (value < 0x0001000000000000l) {
				if (bufferLength - limit < 7) {
					output.flush();
				}
			} else {
				if (value < 0x0080000000000000l) {
					if (bufferLength - limit < 8) {
						output.flush();
					}
				} else {
					if (value < 0x4000000000000000l) {
						if (bufferLength - limit < 9) {
							output.flush();
						}
					} else {
						if (bufferLength - limit < 10) {
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

	public final void writeUnsignedLongNullable(long value) {
		writeUnsignedLong(value+1);
	}
	
	public final void writeUnsignedLong(long value) {

			if (value < 0x0000000000000080l) {
				if (bufferLength - limit < 1) {
					output.flush();
				}
			} else {
				if (value < 0x0000000000004000l) {
					if (bufferLength - limit < 2) {
						output.flush();
					}
				} else {

					if (value < 0x0000000000200000l) {
						if (bufferLength - limit < 3) {
							output.flush();
						}
					} else {

						if (value < 0x0000000010000000l) {
							if (bufferLength - limit < 4) {
								output.flush();
							}
						} else {
							if (value < 0x0000000800000000l) {
						
								if (bufferLength - limit < 5) {
									output.flush();
								}
							} else {
								writeUnsignedLongSlow(value);
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

	private final void writeUnsignedLongSlow(long value) {
		if (value < 0x0000040000000000l) {
			if (bufferLength - limit < 6) {
				output.flush();
			}
		} else {
			writeUnsignedLongSlow2(value);
		}

		// used by all
		buffer[limit++] = (byte) (((value >> 35) & 0x7F));
		buffer[limit++] = (byte) (((value >> 28) & 0x7F));
		buffer[limit++] = (byte) (((value >> 21) & 0x7F));
		buffer[limit++] = (byte) (((value >> 14) & 0x7F));
		buffer[limit++] = (byte) (((value >> 7) & 0x7F));
		buffer[limit++] = (byte) (((value & 0x7F) | 0x80));

	}

	private void writeUnsignedLongSlow2(long value) {
		if (value < 0x0002000000000000l) {
			if (bufferLength - limit < 7) {
				output.flush();
			}
		} else {
			if (value < 0x0100000000000000l) {
				if (bufferLength - limit < 8) {
					output.flush();
				}
			} else {
				if (value < 0x8000000000000000l) {
					if (bufferLength - limit < 9) {
						output.flush();
					}
				} else {
					if (bufferLength - limit < 10) {
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
	
	
	public final void writeSignedIntegerNullable(int value) {
		if (value >= 0) { 
			writeSignedIntegerPos(value+1);
		} else {
			if ((value << 1) == 0) {
				if (limit > bufferLength - 5) {
					output.flush();
				}
				// encode the most negative possible number
				buffer[limit++] = (byte) (0x7F); // .... ...7 F... ....
				buffer[limit++] = (byte) (0x00); // .... .... .FE. ....
				buffer[limit++] = (byte) (0x00); // .... .... ...1 FC..
				buffer[limit++] = (byte) (0x00); // .... .... .... 3F8.
				buffer[limit++] = (byte) (0x80); // .... .... .... ..7f
			} else {
				writeSignedIntegerNeg(value);
			}
		}
	}
	
	public final void writeSignedInteger(int value) {
		if (value >= 0) { 
			writeSignedIntegerPos(value);
		} else {
			if ((value << 1) == 0) {
				if (limit > bufferLength - 5) {
					output.flush();
				}
				// encode the most negative possible number
				buffer[limit++] = (byte) (0x7F); // .... ...7 F... ....
				buffer[limit++] = (byte) (0x00); // .... .... .FE. ....
				buffer[limit++] = (byte) (0x00); // .... .... ...1 FC..
				buffer[limit++] = (byte) (0x00); // .... .... .... 3F8.
				buffer[limit++] = (byte) (0x80); // .... .... .... ..7f
			} else {
				writeSignedIntegerNeg(value);
			}
		}
	}

	private void writeSignedIntegerNeg(int value) {
	    // using absolute value avoids tricky word length issues
	    int absv = -value;
	    
	    
		if (absv <= 0x00000040) {
			if (bufferLength - limit < 1) {
				output.flush();
			}
		} else {
			if (absv <= 0x00002000) {
				if (bufferLength - limit < 2) {
					output.flush();
				}
			} else {
				if (absv <= 0x00100000) {
					if (bufferLength - limit < 3) {
						output.flush();
					}
				} else {
					if (absv <= 0x08000000) {
						if (bufferLength - limit < 4) {
							output.flush();
						}
					} else {
						if (bufferLength - limit < 5) {
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

	private void writeSignedIntegerPos(int value) {
		
		if (value < 0x00000040) {
			if (bufferLength - limit < 1) {
				output.flush();
			}
		} else {
			if (value < 0x00002000) {
				if (bufferLength - limit < 2) {
					output.flush();
				}
			} else {
				if (value < 0x00100000) {
					if (bufferLength - limit < 3) {
						output.flush();
					}
				} else {
					if (value < 0x08000000) {
						if (bufferLength - limit < 4) {
							output.flush();
						}
					} else {
						if (bufferLength - limit < 5) {
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
	
	public final void writeUnsignedIntegerNullable(int value) {
		writeUnsignedInteger(value+1);
	}
	
	public final void writeUnsignedInteger(int value) {
		
		if (value < 0x00000080) {
			if (bufferLength - limit < 1) {
				output.flush();
			}
		} else {
			if (value < 0x00004000) {
				if (bufferLength - limit < 2) {
					output.flush();
				}
			} else {
				if (value < 0x00200000) {
					if (bufferLength - limit < 3) {
						output.flush();
					}
				} else {
					if (value < 0x10000000) {
						if (bufferLength - limit < 4) {
							output.flush();
						}
					} else {
						if (bufferLength - limit < 5) {
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
		
		if (limit > bufferLength - maxBytes) {
			output.flush();
		}
		
		//save the current partial byte.
		//always save because pop will always load
		if (safetyStackDepth>0) {			
			int s = safetyStackDepth-1;
			assert(s>=0) : "Must call pushPMap(maxBytes) before attempting to write bits to it";
					
			buffer[safetyStackPosition[s]++] = pMapByteAccum;//final byte to be saved into the feed.
			safetyStackTemp[s] = pMapIdxWorking;
			
			if (0 != pMapByteAccum) {	
				//set the last known non zero bit so we can avoid scanning for it. 
				flushSkips[safetyStackFlushIdx[s]] = safetyStackPosition[s];// one has been added for exclusive use of range
			}	
			
		} 
		//reset so we can start accumulating bits in the new pmap.
		pMapIdxWorking = 7;
		pMapByteAccum = 0;			
		
		safetyStackPosition[safetyStackDepth] = limit;

		safetyStackFlushIdx[safetyStackDepth++] = flushSkipsIdxLimit;
		flushSkips[flushSkipsIdxLimit++] = limit+1;//default minimum size for present PMap
		flushSkips[flushSkipsIdxLimit++] = (limit += maxBytes);//this will remain as the fixed limit					
	}
	
	//called only at the end of a group.
	public final void closePMap() {
		/////
		//the PMap is ready for writing.
		//bit writes will go to previous bitmap location
		/////
		//push open writes
	   	pushWorkingBits(--safetyStackDepth, (byte)0);
		
		
		buffer[flushSkips[safetyStackFlushIdx[safetyStackDepth]] - 1] |= 0x80;//must set stop bit now that we know where pmap stops.
				
		//restore the old working bits if there is a previous pmap.
		if (safetyStackDepth>0) {			
			popWorkingBits(safetyStackDepth-1);
		}
		
		//ensure low-latency for groups, or 
    	//if we can reset the safety stack and we have one block ready go ahead and flush
    	if (minimizeLatency || (0==safetyStackDepth && (limit-position)>(BLOCK_SIZE) )) { //one block and a bit left over so we need bigger.
    		output.flush();
    	}
	}

	private final void popWorkingBits(int s) {
		pMapByteAccum = buffer[safetyStackPosition[s]--];
		pMapIdxWorking = safetyStackTemp[s];
	}

	private final void pushWorkingBits(int s, byte pmIdxWorking) {
		assert(s>=0) : "Must call pushPMap(maxBytes) before attempting to write bits to it";
				
		buffer[safetyStackPosition[s]++] = pMapByteAccum;//final byte to be saved into the feed.
		safetyStackTemp[s] = pmIdxWorking;
		
		if (0 != pMapByteAccum) {	
			//set the last known non zero bit so we can avoid scanning for it. 
			flushSkips[safetyStackFlushIdx[s]] = safetyStackPosition[s];// one has been added for exclusive use of range
		}	

		pMapIdxWorking = 7;
		pMapByteAccum = 0;
	}
	
	//called by ever field that needs to set a bit either 1 or 0
	//must be fast because it is frequently called.
	public final void writePMapBit(int bit) {
		if (0 == --pMapIdxWorking) {
			int s = safetyStackDepth-1; 
			
			assert(s>=0) : "Must call pushPMap(maxBytes) before attempting to write bits to it";
					
			int local = (pMapByteAccum | bit);
			buffer[safetyStackPosition[s]++] = (byte) local;//final byte to be saved into the feed.
			safetyStackTemp[s] = pMapIdxWorking;
			
			if (0 != local) {	
				//set the last known non zero bit so we can avoid scanning for it. 
				flushSkips[safetyStackFlushIdx[s]] = safetyStackPosition[s];// one has been added for exclusive use of range
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

	
	
	
}
