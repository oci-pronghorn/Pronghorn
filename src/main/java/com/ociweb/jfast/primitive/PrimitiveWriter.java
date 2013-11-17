package com.ociweb.jfast.primitive;

import com.ociweb.jfast.primitive.adapter.FASTOutputByteArray;




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
	
	final byte[] buffer;
	private final int bufferLength;
	private final int flushTrigger;
	
	private int position;
	private int limit;
	
	private final int[] safetyStackPosition; //location where the last byte was written to the pmap as bits are written
	private final int[] safetyStackFlushIdx; //location (in skip list) where location of stopBytes+1 and end of max pmap length is found.
	private final byte[] safetyStackTemp; //working bit position 1-7 for next/last bit to be written.
	private int   safetyStackDepth;       //maximum depth of the stacks above
	byte pMapIdxWorking = 7;
	byte pMapByteAccum = 0;
	
	private final int[] flushSkips;//list of all skip nodes produced at the end of pmaps, may grow large with poor templates.
	
	private int   flushSkipsIdxLimit; //where we add the new one, end of the list
	private int   flushSkipsIdxPos;//next limit to use. as skips are consumed this pointer moves forward.

	private long totalWritten;
	
	private final boolean minimizeLatency;
		
	private FASTOutputByteArray test;

	public PrimitiveWriter(FASTOutput output) {
		this(4096,output,128, false);
	}
	
	public PrimitiveWriter(int initBufferSize, FASTOutput output, int maxGroupCount, boolean minimizeLatency) {

		//TODO: using the crazy expnsive put methods of something like this will hurt in memory
		//however they will allow for faster io write so this needs to be tested at some point.
		//TODO: wrap all array access in an internal method so I can easily test this.
		//ByteBuffer buf = ByteBuffer.allocateDirect(initBufferSize);
		//buf.put(b)
		
		this.buffer = new byte[initBufferSize];
		this.bufferLength = buffer.length;
		this.flushTrigger = Math.min(1024, bufferLength>>2);
		this.position = 0;
		this.limit = 0;
		this.minimizeLatency = minimizeLatency;
		//NOTE: may be able to optimize this so these 3 are shorter
		safetyStackPosition = new int[maxGroupCount];
		safetyStackFlushIdx = new int[maxGroupCount];
		safetyStackTemp = new byte[maxGroupCount];
		//max total groups
		flushSkips = new int[maxGroupCount*2];//this may grow very large, to fields per group
	
		this.output = output;
		
		if (output instanceof FASTOutputByteArray) {
			test = (FASTOutputByteArray) output;
		}
		
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
 	
    public final void flush () { //flush all 
    	flush(0);
    }
    
    //only call if caller is SURE it must be done.
	private final void flush (int need) {
		//this needs to be made smaller to allow for inline!!

		//System.err.println("simple flush");
		
		//compute the flushTo value from limit and the stack data
		int flushTo = computeFlushToIndex();		
		
		int toFlush = flushTo-position;
		if (0==toFlush) {
			//no need to do any work.
			return;
		}
		
		int flushed = 0;
		if (flushSkipsIdxPos==flushSkipsIdxLimit) {
			//nothing to skip so flush the full block
			flushed =  output.flush(buffer, position, flushTo-position);
			position += flushed;
			
			if (flushed<4 && bufferLength-limit>need) {
				new Exception("WASTED flush: potential:"+toFlush+" "+position+" "+limit).printStackTrace();
			}
			
		} else {
			if (0==need || minimizeLatency){
				//write as little as one byte
				flushed = flushWithSkips(flushTo, 0, flushSkipsIdxLimit);
			} else {
				//save up flushed to do gathering
				if (toFlush>flushTrigger || position>flushTrigger) {
					flushed = flushWithSkips(flushTo, 0, flushSkipsIdxLimit);
					//System.err.println("flushed:"+flushed);
				} else {
					flushed = 0;
				}
			}
			//reset to zero to save space if possible
			if (flushSkipsIdxPos==flushSkipsIdxLimit) {
				flushSkipsIdxPos=flushSkipsIdxLimit=0;
				
				//can only be done when there are no remaining skips.
				//if this is not written it will align with the next flush to optimize bandwidth
				//if this is written then this data will have lower latency.
				if (minimizeLatency) {
					//nothing to skip so flush the full block
					int temp = output.flush(buffer, position, flushTo-position);
					flushed += temp; 
					position += temp;
				}
				
			}
		}

		totalWritten+=flushed; 

		//when possible try to move back down the buffer to use the old space.
		//this can only happen under special conditions where all the data has been flushed
		//and nothing is pending.
		if (position == limit &&
		    0 == safetyStackDepth &&
			0 == flushSkipsIdxLimit) { 
			position = limit = 0;
		}			
		
		if (bufferLength - limit < need) {
			throw new UnsupportedOperationException("Unable to flush due to pending pmaps. need:"+need+" buf:"+bufferLength+" lim:"+limit+" pos:"+position);
		}
		
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
	
//	boolean flushAll = false;
	
	
	private int flushWithSkips(final int flushTo, int need, int skipsIdxLimit) {
		int rollingPos = position;
		int flushed = 0;
		
		byte[] b = buffer;
		int[] localFlushSkips = flushSkips;
		int localFlushSkipsIdxPos = flushSkipsIdxPos;
		int tempSkipPos = localFlushSkips[localFlushSkipsIdxPos];
		int lastValid = localFlushSkips.length-2; 
		if (skipsIdxLimit<lastValid) {
			lastValid = skipsIdxLimit;
		} 
		
		//flush in parts that avoid the skip pos
		while (localFlushSkipsIdxPos < lastValid && //we still have skips
				tempSkipPos < flushTo                  //skip stops before goal
				) { 
			
			//if the skip is zero bytes just flush it all together
			if (localFlushSkips[localFlushSkipsIdxPos+1]==tempSkipPos) {
					++localFlushSkipsIdxPos;
					tempSkipPos = localFlushSkips[++localFlushSkipsIdxPos];
			}
			
			int flushRequest = tempSkipPos - rollingPos;
			int flushComplete;
			
			if (null!=test) {
				////////////////////////////////////////
				//TODO: this block will not remain I am just testing the performance of inlineing this work.
				///////////////////////////////////////
				
				if (1==flushRequest) {
					test.buffer[test.position++] = b[rollingPos];
				} else {
				
					System.arraycopy(b,rollingPos,test.buffer,test.position,flushRequest);
					test.position+=flushRequest;
				}
				flushComplete = flushRequest;//test.flush(b, rollingPos, flushRequest);
				
			} else {
			
				flushComplete = output.flush(b, rollingPos, flushRequest);
			
			}
			flushed += flushComplete;
			//did flush up to skip so set rollingPos to after skip
			rollingPos = localFlushSkips[++localFlushSkipsIdxPos]; //new position in second part of flush skips
			tempSkipPos = localFlushSkips[++localFlushSkipsIdxPos]; //beginning of new skip.
			
			if (flushComplete < flushRequest) {
				//we are getting back pressure so stop flushing any more
				break;
			}	
		} 
		
		position = rollingPos;		
		flushSkipsIdxPos = localFlushSkipsIdxPos;

		return flushed;
	}
		

	//this requires the null adjusted length to be written first.
	public final void writeByteArrayData(byte[] data) {
		final int len = data.length;
		if (limit>bufferLength-len) {
			flush(len);
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
				flush(2);
			}
			buffer[limit++] = (byte)0;
			buffer[limit++] = (byte)0x80;
			return;
		}
		if (limit>bufferLength-length) {
			flush(length);
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
			flush(1);
		}
		buffer[limit++] = (byte)0x80;
	}
	
	public final void writeSignedLongNullable(long value) {

		if (value >= 0) {
			writeSignedLongPos(value+1);
		} else {

			if ((value << 1) == 0) {
				if (limit > bufferLength - 10) {
					flush(10);
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
					flush(10);
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
				flush(1);
			}
		} else {
			if (absv <= 0x0000000000002000l) {
				if (bufferLength - limit < 2) {
					flush(2);
				}
			} else {

				if (absv <= 0x0000000000100000l) {
					if (bufferLength - limit < 3) {
						flush(3);
					}
				} else {

					if (absv <= 0x0000000008000000l) {
						if (bufferLength - limit < 4) {
							flush(4);
						}
					} else {
						if (absv <= 0x0000000400000000l) {
					
							if (bufferLength - limit < 5) {
								flush(5);
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
				flush(6);
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
				flush(7);
			}
		} else {
			if (absv <= 0x0080000000000000l) {
				if (bufferLength - limit < 8) {
					flush(8);
				}
			} else {
				if (bufferLength - limit < 9) {
					flush(9);
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
				flush(1);
			}
		} else {
			if (value < 0x0000000000002000l) {
				if (bufferLength - limit < 2) {
					flush(2);
				}
			} else {

				if (value < 0x0000000000100000l) {
					if (bufferLength - limit < 3) {
						flush(3);
					}
				} else {

					if (value < 0x0000000008000000l) {
						if (bufferLength - limit < 4) {
							flush(4);
						}
					} else {
						if (value < 0x0000000400000000l) {
					
							if (bufferLength - limit < 5) {
								flush(5);
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
				flush(6);
			}
		} else {
			if (value < 0x0001000000000000l) {
				if (bufferLength - limit < 7) {
					flush(7);
				}
			} else {
				if (value < 0x0080000000000000l) {
					if (bufferLength - limit < 8) {
						flush(8);
					}
				} else {
					if (value < 0x4000000000000000l) {
						if (bufferLength - limit < 9) {
							flush(9);
						}
					} else {
						if (bufferLength - limit < 10) {
							flush(10);
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
					flush(1);
				}
			} else {
				if (value < 0x0000000000004000l) {
					if (bufferLength - limit < 2) {
						flush(2);
					}
				} else {

					if (value < 0x0000000000200000l) {
						if (bufferLength - limit < 3) {
							flush(3);
						}
					} else {

						if (value < 0x0000000010000000l) {
							if (bufferLength - limit < 4) {
								flush(4);
							}
						} else {
							if (value < 0x0000000800000000l) {
						
								if (bufferLength - limit < 5) {
									flush(5);
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
				flush(6);
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
				flush(7);
			}
		} else {
			if (value < 0x0100000000000000l) {
				if (bufferLength - limit < 8) {
					flush(8);
				}
			} else {
				if (value < 0x8000000000000000l) {
					if (bufferLength - limit < 9) {
						flush(9);
					}
				} else {
					if (bufferLength - limit < 10) {
						flush(10);
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
					flush(5);
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
					flush(5);
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
				flush(1);
			}
		} else {
			if (absv <= 0x00002000) {
				if (bufferLength - limit < 2) {
					flush(2);
				}
			} else {
				if (absv <= 0x00100000) {
					if (bufferLength - limit < 3) {
						flush(3);
					}
				} else {
					if (absv <= 0x08000000) {
						if (bufferLength - limit < 4) {
							flush(4);
						}
					} else {
						if (bufferLength - limit < 5) {
							flush(5);
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
				flush(1);
			}
		} else {
			if (value < 0x00002000) {
				if (bufferLength - limit < 2) {
					flush(2);
				}
			} else {
				if (value < 0x00100000) {
					if (bufferLength - limit < 3) {
						flush(3);
					}
				} else {
					if (value < 0x08000000) {
						if (bufferLength - limit < 4) {
							flush(4);
						}
					} else {
						if (bufferLength - limit < 5) {
							flush(5);
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
				flush(1);
			}
		} else {
			if (value < 0x00004000) {
				if (bufferLength - limit < 2) {
					flush(2);
				}
			} else {
				if (value < 0x00200000) {
					if (bufferLength - limit < 3) {
						flush(3);
					}
				} else {
					if (value < 0x10000000) {
						if (bufferLength - limit < 4) {
							flush(4);
						}
					} else {
						if (bufferLength - limit < 5) {
							flush(5);
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
			flush(maxBytes);
		}
		
		//save the current partial byte.
		//always save because pop will always load
		if (safetyStackDepth>0) {			
			pushWorkingBits(safetyStackDepth-1, pMapIdxWorking);
		} else {
			//reset so we can start accumulating bits in the new pmap.
			//not sure what old values might be in here so clear it.
			pMapIdxWorking = 7;
			pMapByteAccum = 0;			
		}
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
		//ensure low-latency for groups, or flush if we have no open groups
    	if (minimizeLatency) {
    		flush(0);
    	}
    	if (0==safetyStackDepth) {
    		flush(1);
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
