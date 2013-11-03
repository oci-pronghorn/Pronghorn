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
    
	public final void flush (int need) {
		
		int flushed =  output.flush(buffer, position, limit);
		totalWritten+=flushed;
		//if this is not true not all the bytes were written.
		int newPosition = position+flushed;
		if (newPosition==limit) {
			//flushed all
			position = limit = 0;
		} else {
			position = newPosition;
		}
		
		if (buffer.length-limit<need) {
			growBuffer(need);
		}
		
	}
	
	private final void growBuffer(int required) {
		//System.err.println("grow buffer :"+required);
		//must grow buffer to fit required bytes
		int newSize;
		//double the larger value
		if (required>buffer.length) {
			newSize = required*2;
		} else {
			newSize = buffer.length*2;
		}
		System.err.println("new buffer :"+newSize);
		byte[] newBuffer = new byte[newSize];
		System.arraycopy(buffer, position, newBuffer, 0, limit-position);
		buffer = newBuffer;
		limit = limit - position;
		position = 0;
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

	//PMap spport
	
	//called in each field to add bit
	public void pushPMapbit(int i) {
		// TODO Auto-generated method stub
		
	}
	
	//called at end of writing group
	public void writePMap() {
		
	}
	
	//called at beginning of group to start a new pmap.
	public void pushPMap() {
		
	}
	
	
}
