package com.ociweb.jpgRaster.r2j;

import java.util.ArrayList;

import com.ociweb.jpgRaster.JPG;
import com.ociweb.jpgRaster.JPGSchema;
import com.ociweb.jpgRaster.JPG.Header;
import com.ociweb.jpgRaster.JPG.HuffmanTable;
import com.ociweb.jpgRaster.JPG.MCU;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class HuffmanEncoder extends PronghornStage {
	
	private static class BitWriter {
		private int nextByte = 0;
		private int nextBit = 0;
		private ArrayList<Byte> data = new ArrayList<Byte>();
		
//		private int[] masks = new int[]{1, 3, 7, 15, 31, 63, 127, 255, 511, 1023, 2047, 4095, 8191, 16383};
		
		public BitWriter() {
			data.add((byte)0);
		}
		
		public void putBit(int x) {
			
			if (nextBit > 7) {
				data.add((byte) 0);
				nextBit = 0;
				nextByte++;
			}
			
			data.set(nextByte, (byte)((data.get(nextByte) << 1) | (x & 1)));
			nextBit++;
		}
		
		public void putBits(int x, int length) {
			
			for(int i = 0; i < length; ++i) {
				putBit(x);
				x = x >> 1;
			}
		}
		
		public void printData() {
			for (int i = 0; i < data.size(); ++i) {
				System.out.print(String.format("%8s", Integer.toBinaryString(data.get(i) & 0xFF)).replace(" ", "0") + "  ");
			}
		}
		
		public void restart() {
			data.clear();
			data.add((byte) 0);
		}
		
		/*public boolean hasBits() {
			
		}*/
		
		/*public void align() {
			
		}*/
	}
	
	public static void main(String[] args) {
		BitWriter b = new BitWriter();
		
		b.putBit(1);
		b.putBit(0);
		b.putBit(1);
		b.putBit(0);
		b.putBit(1);
		b.putBit(0);
		b.putBit(0);
		b.putBit(1);
		b.putBit(1);
		
		b.putBit(1);
		b.putBit(0);
		b.putBit(1);
		b.putBit(0);
		b.putBit(1);
		b.putBit(0);
		b.putBit(0);
		b.putBit(1);
		b.putBit(1);
		
		b.putBit(1);
		b.putBit(0);
		b.putBit(1);
		b.putBit(0);
		b.putBit(1);
		b.putBit(0);
		b.putBit(0);
		b.putBit(1);
		b.putBit(1);
		
		b.putBits(7, 3);
		b.putBits(0, 2);
		
		b.printData();
	}
	
	private final Pipe<JPGSchema> input;
	boolean verbose;
	
	Header header;
	MCU mcu = new MCU();
	
	public HuffmanEncoder(GraphManager graphManager, Pipe<JPGSchema> input, boolean verbose) {
		super(graphManager, input, NONE);
		this.input = input;
		this.verbose = verbose;
	}

	static short previousYDC = 0;
	static short previousCbDC = 0;
	static short previousCrDC = 0;

	static BitWriter b;

	private static boolean encodeMCUComponent(
			  ArrayList<ArrayList<Integer>> DCTableCodes,
			  ArrayList<ArrayList<Integer>> ACTableCodes,
			  HuffmanTable DCTable,
			  HuffmanTable ACTable,
			  short[] component,
			  short previousDC) {



		return true;
	}


	public static void encodeHuffmanData(MCU mcu) {
		encodeMCUComponent(JPG.DCTableCodes0, JPG.ACTableCodes0, JPG.hDCTable0, JPG.hACTable0, mcu.y, previousYDC);

		encodeMCUComponent(JPG.DCTableCodes1, JPG.ACTableCodes1, JPG.hDCTable1, JPG.hACTable1, mcu.cb, previousCbDC);

		encodeMCUComponent(JPG.DCTableCodes1, JPG.ACTableCodes1, JPG.hDCTable1, JPG.hACTable1, mcu.cr, previousCrDC);
	}


	@Override
	public void run() {
		while (PipeReader.tryReadFragment(input)) {

			int msgIdx = PipeReader.getMsgIdx(input);

			if (msgIdx == JPGSchema.MSG_HEADERMESSAGE_1) {
				// read header from pipe
				header = new Header();
				header.height = PipeReader.readInt(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_HEIGHT_101);
				header.width = PipeReader.readInt(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_WIDTH_201);
				header.filename = PipeReader.readASCII(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_FILENAME_301, new StringBuilder()).toString();
				PipeReader.releaseReadLock(input);
			}
			else if (msgIdx == JPGSchema.MSG_MCUMESSAGE_4) {
				DataInputBlobReader<JPGSchema> mcuReader = PipeReader.inputStream(input, JPGSchema.MSG_MCUMESSAGE_4_FIELD_Y_104);
				for (int i = 0; i < 64; ++i) {
					mcu.y[i] = mcuReader.readShort();
				}

				for (int i = 0; i < 64; ++i) {
					mcu.cb[i] = mcuReader.readShort();
				}

				for (int i = 0; i < 64; ++i) {
					mcu.cr[i] = mcuReader.readShort();
				}
				PipeReader.releaseReadLock(input);

				encodeHuffmanData(mcu);
			}
			else {
				System.err.println("Huffman Encoder requesting shutdown");
				requestShutdown();
			}
		}
	}

}
