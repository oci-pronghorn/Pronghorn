package com.ociweb.jfast.primitive;

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
	
	private byte[] buffer;
	private int position;
	private int limit;
	
	
	//new development for pmap writing
	//this will be simplified after it works.
	
	
	//TODO: can remove two of these stacks.
	private int[] safetyStackBegin = new int[128]; //TODO; grow to needed depths?
	private int[] safetyStackPosition = new int[128];
	private int[] safetyStackFlushIdx = new int[128];
	private int   safetyStackDepth;        	//never flush after this point
	byte pMapIdxWorking = 7;
	byte pMapByteAccum = 0;
	
	private int[] flushSkips = new int[128];//this may grow very large
	private int   flushSkipsLimit; //where we add the new one.
	private int   flushSkipsPos;//next limit to use.

	//TODO: if pos = limit then start flushSkips at 0 again?
	//TODO: do move buffer if the safety stack is zero.
	
	
	
	private long totalWritten;
		
	public PrimitiveWriter(FASTOutput output) {
		this(4096,output);
	}
	
	public PrimitiveWriter(int initBufferSize, FASTOutput output) {
		this.output = output;
		this.buffer = new byte[initBufferSize];
		this.position = 0;
		this.limit = 0;
	}
	
    public long totalWritten() {
    	return totalWritten;
    }
	
    public final void flush () {
    	flush(0);
    }
    
    //only call if caller is SURE it must be done.
	public final void flush (int need) {
		
		int flushTo = limit;
		if (safetyStackDepth>0) {
			//only need to check first entry on stack the rest are larger values
			int safetyLimit = safetyStackBegin[0];
			if (safetyLimit<flushTo) {
				flushTo = safetyLimit;
			}		
		}		
		
		int flushed = 0;
		
		if (flushSkipsPos==flushSkipsLimit) {
			//nothing to skip so flush the full block
			flushed =  output.flush(buffer, position, flushTo);
		} else {
			int rollingPos = position;
			//flush in parts that avoid the skip pos
			while (flushSkipsLimit>flushSkipsPos && flushSkips[flushSkipsPos]<flushTo) {
				//we have skip bytes make sure they are enforced.
				
				int flushRequest = flushSkips[flushSkipsPos] - rollingPos;
				int flushComplete =  output.flush(buffer, rollingPos, flushSkips[flushSkipsPos]);
				flushed =  (flushComplete+rollingPos) - position;
				if (flushComplete != flushRequest) {
					//must quit early if any flush does not write all expected
					break;
				}
				//did flush up to skip so set rollingPos to after skip
				rollingPos = flushSkips[++flushSkipsPos];
				flushSkipsPos++;
			} 
			if (flushSkipsPos==flushSkipsLimit) {
				flushSkipsPos=flushSkipsLimit=0;
			}
		}
		
		totalWritten+=flushed;
		//if this is not true not all the bytes were written.
		int newPosition = position+flushed;
		if (newPosition==limit) {
			//flushed all
			position = limit = 0;
		} else {
			position = newPosition;
		}
		
		int totalReq = limit+need;
		if (totalReq > buffer.length) {
			growBuffer(totalReq);
		}
		
	}
	
	private int[] growIntegers(int[] old, int need) {
		int newSize = need*2;
		int[] newArray = new int[newSize];
		System.arraycopy(old, 0, newArray, 0, old.length);
		return newArray;
	}
	
	private final void growBuffer(int required) {
		//System.err.println("grow buffer :"+required);
		//must grow buffer to fit required bytes
		int newSize = required*2;

		System.err.println("new write buffer :"+newSize);
		
		byte[] newBuffer = new byte[newSize];
		//do not shift the data in the buffer down because pmap stacks
		//have indexes into these locations. TODO: it it worth finding a way to do this?
		System.arraycopy(buffer, position, newBuffer, position, limit-position);
		buffer = newBuffer;
		//limit and position will remain unchanged.
	}
	
	//this requires the null adjusted length to be written first.
	public final void writeByteArrayData(byte[] data) {
		final int len = data.length;
		if (limit>buffer.length-len) {
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
			if (limit>buffer.length-2) {
				flush(2);
			}
			buffer[limit++] = (byte)0;
			buffer[limit++] = (byte)0x80;
			return;
		}
		if (limit>buffer.length-length) {
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
		if (limit>=buffer.length) {
			flush(1);
		}
		buffer[limit++] = (byte)0x80;
	}
	
	public final void writeSignedLongNullable(long value) {

		if (value >= 0) {
			writeSignedLongPos(value+1);
		} else {

			if ((value << 1) == 0) {
				if (limit > buffer.length - 10) {
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
				if (limit > buffer.length - 10) {
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
			if (buffer.length - limit < 1) {
				flush(1);
			}
		} else {
			if (absv <= 0x0000000000002000l) {
				if (buffer.length - limit < 2) {
					flush(2);
				}
			} else {

				if (absv <= 0x0000000000100000l) {
					if (buffer.length - limit < 3) {
						flush(3);
					}
				} else {

					if (absv <= 0x0000000008000000l) {
						if (buffer.length - limit < 4) {
							flush(4);
						}
					} else {
						if (absv <= 0x0000000400000000l) {
					
							if (buffer.length - limit < 5) {
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
			if (buffer.length - limit < 6) {
				flush(6);
			}
		} else {
			if (absv <= 0x0001000000000000l) {
				if (buffer.length - limit < 7) {
					flush(7);
				}
			} else {
				if (absv <= 0x0080000000000000l) {
					if (buffer.length - limit < 8) {
						flush(8);
					}
				} else {
					if (buffer.length - limit < 9) {
						flush(9);
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

	private final void writeSignedLongPos(long value) {
		
		if (value < 0x0000000000000040l) {
			if (buffer.length - limit < 1) {
				flush(1);
			}
		} else {
			if (value < 0x0000000000002000l) {
				if (buffer.length - limit < 2) {
					flush(2);
				}
			} else {

				if (value < 0x0000000000100000l) {
					if (buffer.length - limit < 3) {
						flush(3);
					}
				} else {

					if (value < 0x0000000008000000l) {
						if (buffer.length - limit < 4) {
							flush(4);
						}
					} else {
						if (value < 0x0000000400000000l) {
					
							if (buffer.length - limit < 5) {
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
			if (buffer.length - limit < 6) {
				flush(6);
			}
		} else {
			if (value < 0x0001000000000000l) {
				if (buffer.length - limit < 7) {
					flush(7);
				}
			} else {
				if (value < 0x0080000000000000l) {
					if (buffer.length - limit < 8) {
						flush(8);
					}
				} else {
					if (value < 0x4000000000000000l) {
						if (buffer.length - limit < 9) {
							flush(9);
						}
					} else {
						if (buffer.length - limit < 10) {
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
				if (buffer.length - limit < 1) {
					flush(1);
				}
			} else {
				if (value < 0x0000000000004000l) {
					if (buffer.length - limit < 2) {
						flush(2);
					}
				} else {

					if (value < 0x0000000000200000l) {
						if (buffer.length - limit < 3) {
							flush(3);
						}
					} else {

						if (value < 0x0000000010000000l) {
							if (buffer.length - limit < 4) {
								flush(4);
							}
						} else {
							if (value < 0x0000000800000000l) {
						
								if (buffer.length - limit < 5) {
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
			if (buffer.length - limit < 6) {
				flush(6);
			}
		} else {
			if (value < 0x0002000000000000l) {
				if (buffer.length - limit < 7) {
					flush(7);
				}
			} else {
				if (value < 0x0100000000000000l) {
					if (buffer.length - limit < 8) {
						flush(8);
					}
				} else {
					if (value < 0x8000000000000000l) {
						if (buffer.length - limit < 9) {
							flush(9);
						}
					} else {
						if (buffer.length - limit < 10) {
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
	
	//TODO: write signed int for decimals these are between -63 and +63 inclusive!
	//all in one method and inlined together.
	public final void writeSignedInt6(int value) {
		assert(value>=-63 && value<=63);

		if (limit>=buffer.length) {
			flush(1);
		}
		
		buffer[limit++] = (byte) (((value & 0x7F) | 0x80));
		
		//TODO: needs test.
		//what about nulled? does  that cause a problem?
		
	}
	
	public final void writeSignedIntegerNullable(int value) {
		if (value >= 0) { 
			writeSignedIntegerPos(value+1);
		} else {
			if ((value << 1) == 0) {
				if (limit > buffer.length - 5) {
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
				if (limit > buffer.length - 5) {
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
			if (buffer.length - limit < 1) {
				flush(1);
			}
		} else {
			if (absv <= 0x00002000) {
				if (buffer.length - limit < 2) {
					flush(2);
				}
			} else {
				if (absv <= 0x00100000) {
					if (buffer.length - limit < 3) {
						flush(3);
					}
				} else {
					if (absv <= 0x08000000) {
						if (buffer.length - limit < 4) {
							flush(4);
						}
					} else {
						if (buffer.length - limit < 5) {
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
			if (buffer.length - limit < 1) {
				flush(1);
			}
		} else {
			if (value < 0x00002000) {
				if (buffer.length - limit < 2) {
					flush(2);
				}
			} else {
				if (value < 0x00100000) {
					if (buffer.length - limit < 3) {
						flush(3);
					}
				} else {
					if (value < 0x08000000) {
						if (buffer.length - limit < 4) {
							flush(4);
						}
					} else {
						if (buffer.length - limit < 5) {
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
			if (buffer.length - limit < 1) {
				flush(1);
			}
		} else {
			if (value < 0x00004000) {
				if (buffer.length - limit < 2) {
					flush(2);
				}
			} else {
				if (value < 0x00200000) {
					if (buffer.length - limit < 3) {
						flush(3);
					}
				} else {
					if (value < 0x10000000) {
						if (buffer.length - limit < 4) {
							flush(4);
						}
					} else {
						if (buffer.length - limit < 5) {
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
	public void pushPMap(int maxBytes) {
		//////
		//write max bytes of zeros but
		//mark the bytes so they are not flushed out.
	    //move pointer forward for more writes
		//////
		
		maxBytes++;//need one more spot for saving the bitIdx
		if (limit > buffer.length-maxBytes) {
			flush(maxBytes);
		}
		
		//save the current partial byte.
		if (safetyStackDepth>0) {
			int idx = safetyStackPosition[safetyStackDepth-1];
			buffer[idx] = pMapByteAccum;//all bits set at once
			buffer[idx+1] = pMapIdxWorking;
			safetyStackPosition[safetyStackDepth-1] = idx+1;
		}
		//reset so we can start accumulating bits in the new pmap.
		pMapIdxWorking = 7;
		pMapByteAccum = 0;
		
		//push this new safety on the stack
		//beginning and end of the pmap
		safetyStackBegin[safetyStackDepth] = limit;
		safetyStackPosition[safetyStackDepth] = limit;
		safetyStackFlushIdx[safetyStackDepth++] = flushSkipsLimit;
		
		flushSkips[flushSkipsLimit++] = limit;//this will be changed
		limit += maxBytes;	
		flushSkips[flushSkipsLimit++] = limit;//this will remain.
				
		
	}
	
	//called only at the end of a group.
	public void popPMap() {
		/////
		//the PMap is ready for writing.
		//bit writes will go to previous bitmap location
		/////
		
		{ //push open writes
			int s = safetyStackDepth-1;
			int idx = safetyStackPosition[s];
			buffer[idx] = pMapByteAccum;//all bits set at once
			safetyStackPosition[s] = idx+1; //inc to next byte
			
			pMapIdxWorking = 7;
			pMapByteAccum = 0;
		}
		
		
		int begin = safetyStackBegin[--safetyStackDepth];		
		int pos = safetyStackPosition[safetyStackDepth];
		while (pos>begin && buffer[pos]==0) {
			pos--;//do not write the trailing zeros
		}
		buffer[pos] |= 0x80;//must set stop bit now that we know where pmap stops.
		
		int flushIdx = safetyStackFlushIdx[safetyStackDepth];
		flushSkips[flushIdx] = pos+1;//plus one to make this the exclusive value
		
		//restore the old working bits if there is a previous pmap.
		if (safetyStackDepth>0) {
			pMapByteAccum = buffer[safetyStackPosition[safetyStackDepth-1]]; 
			pMapIdxWorking = buffer[safetyStackPosition[safetyStackDepth-1]+1];
		}
		
	}
	
	//called by ever field that needs to set a bit either 1 or 0
	//must be fast because it is frequently called.
	public void writePMapBit(int bit) {
		
		pMapIdxWorking--;
		pMapByteAccum |= (bit<<pMapIdxWorking); 
		
		if (0==pMapIdxWorking) {
			//push this full byte and reset
			int s = safetyStackDepth-1;
			int idx = safetyStackPosition[s];
			buffer[idx] = pMapByteAccum;//all bits set at once
			safetyStackPosition[s] = idx+1; //inc to next byte
			
			pMapIdxWorking = 7;
			pMapByteAccum = 0;
		}
	}
	
	
	
	
	
}
