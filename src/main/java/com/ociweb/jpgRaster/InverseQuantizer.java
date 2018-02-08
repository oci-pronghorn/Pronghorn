package com.ociweb.jpgRaster;

import com.ociweb.jpgRaster.JPG.Header;
import com.ociweb.jpgRaster.JPG.ColorComponent;
import com.ociweb.jpgRaster.JPG.QuantizationTable;
import com.ociweb.jpgRaster.JPG.MCU;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

import java.nio.ByteBuffer;

public class InverseQuantizer extends PronghornStage {

	private final Pipe<JPGSchema> input;
	private final Pipe<JPGSchema> output;
	
	Header header;
	
	protected InverseQuantizer(GraphManager graphManager, Pipe<JPGSchema> input, Pipe<JPGSchema> output) {
		super(graphManager, input, output);
		this.input = input;
		this.output = output;
	}
	
	private static void dequantizeMCU(short[] MCU, QuantizationTable table) {
		/*System.out.print("Before Inverse Quantization:");
		for (int i = 0; i < 8; ++i) {
			for (int j = 0; j < 8; ++j) {
				if (j % 8 == 0) {
					System.out.println();
				}
				System.out.print(MCU[i * 8 + j] + " ");
			}
		}
		System.out.println();*/
		
		for (int i = 0; i < MCU.length; ++i) {
			// type casting is unsafe for 16-bit precision quantization tables
			MCU[i] = (short)(MCU[i] * table.table[i]);
		}
		
		/*System.out.print("After Inverse Quantization:");
		for (int i = 0; i < 8; ++i) {
			for (int j = 0; j < 8; ++j) {
				if (j % 8 == 0) {
					System.out.println();
				}
				System.out.print(MCU[i * 8 + j] + " ");
			}
		}
		System.out.println();*/
	}
	
	public static void dequantize(MCU mcu, Header header) {
		dequantizeMCU(mcu.y, header.quantizationTables.get(header.colorComponents.get(0).quantizationTableID));
		dequantizeMCU(mcu.cb, header.quantizationTables.get(header.colorComponents.get(1).quantizationTableID));
		dequantizeMCU(mcu.cr, header.quantizationTables.get(header.colorComponents.get(2).quantizationTableID));
		return;
	}

	@Override
	public void run() {
		if (PipeWriter.hasRoomForWrite(output) && PipeReader.tryReadFragment(input)) {
			
			int msgIdx = PipeReader.getMsgIdx(input);
			
			if (msgIdx == JPGSchema.MSG_HEADERMESSAGE_1) {
				// read header from pipe
				header = new Header();
				header.height = PipeReader.readInt(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_HEIGHT_101);
				header.width = PipeReader.readInt(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_WIDTH_201);
				String filename = PipeReader.readASCII(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_FILENAME_301, new StringBuilder()).toString();
				header.frameType = PipeReader.readASCII(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_FRAMETYPE_401, new StringBuilder()).toString();
				header.precision = (short) PipeReader.readInt(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_PRECISION_501);
				header.startOfSelection = (short) PipeReader.readInt(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_STARTOFSELECTION_601);
				header.endOfSelection = (short) PipeReader.readInt(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_ENDOFSELECTION_701);
				header.successiveApproximation = (short) PipeReader.readInt(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_SUCCESSIVEAPPROXIMATION_801);
				PipeReader.releaseReadLock(input);

				// write header to pipe
				if (PipeWriter.tryWriteFragment(output, JPGSchema.MSG_HEADERMESSAGE_1)) {
					System.out.println("Writing header to pipe...");
					PipeWriter.writeInt(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_HEIGHT_101, header.height);
					PipeWriter.writeInt(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_WIDTH_201, header.width);
					PipeWriter.writeASCII(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_FILENAME_301, filename);
					PipeWriter.writeASCII(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_FRAMETYPE_401, header.frameType);
					PipeWriter.writeInt(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_PRECISION_501, header.precision);
					PipeWriter.writeInt(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_STARTOFSELECTION_601, header.startOfSelection);
					PipeWriter.writeInt(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_ENDOFSELECTION_701, header.endOfSelection);
					PipeWriter.writeInt(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_SUCCESSIVEAPPROXIMATION_801, header.successiveApproximation);
					PipeWriter.publishWrites(output);
				}
				else {
					System.err.println("Inverse Quantizer requesting shutdown");
					requestShutdown();
				}
			}
			else if (msgIdx == JPGSchema.MSG_COLORCOMPONENTMESSAGE_2) {
				// read color component data from pipe
				ColorComponent component = new ColorComponent();
				component.componentID = (short) PipeReader.readInt(input, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_COMPONENTID_102);
				component.horizontalSamplingFactor = (short) PipeReader.readInt(input, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_HORIZONTALSAMPLINGFACTOR_202);
				component.verticalSamplingFactor = (short) PipeReader.readInt(input, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_VERTICALSAMPLINGFACTOR_302);
				component.quantizationTableID = (short) PipeReader.readInt(input, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_QUANTIZATIONTABLEID_402);
				component.huffmanACTableID = (short) PipeReader.readInt(input, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_HUFFMANACTABLEID_502);
				component.huffmanDCTableID = (short) PipeReader.readInt(input, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_HUFFMANDCTABLEID_602);
				header.colorComponents.add(component);
				PipeReader.releaseReadLock(input);
				
				// write color component data to pipe
				System.out.println("Attempting to write color component to pipe...");
				if (PipeWriter.tryWriteFragment(output, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2)) {
					System.out.println("Writing color component to pipe...");
					PipeWriter.writeInt(output, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_COMPONENTID_102, component.componentID);
					PipeWriter.writeInt(output, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_HORIZONTALSAMPLINGFACTOR_202, component.horizontalSamplingFactor);
					PipeWriter.writeInt(output, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_VERTICALSAMPLINGFACTOR_302, component.verticalSamplingFactor);
					PipeWriter.writeInt(output, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_QUANTIZATIONTABLEID_402, component.quantizationTableID);
					PipeWriter.writeInt(output, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_HUFFMANACTABLEID_502, component.huffmanACTableID);
					PipeWriter.writeInt(output, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_HUFFMANDCTABLEID_602, component.huffmanDCTableID);
					PipeWriter.publishWrites(output);
				}
				else {
					System.err.println("Inverse Quantizer requesting shutdown");
					requestShutdown();
				}
			}
			else if (msgIdx == JPGSchema.MSG_QUANTIZATIONTABLEMESSAGE_5) {
				// read quantization table from pipe
				QuantizationTable table = new QuantizationTable();
				table.tableID = (short) PipeReader.readInt(input, JPGSchema.MSG_QUANTIZATIONTABLEMESSAGE_5_FIELD_TABLEID_105);
				table.precision = (short) PipeReader.readInt(input, JPGSchema.MSG_QUANTIZATIONTABLEMESSAGE_5_FIELD_PRECISION_205);
				ByteBuffer buffer = ByteBuffer.allocate(64 * 4);
				PipeReader.readBytes(input, JPGSchema.MSG_QUANTIZATIONTABLEMESSAGE_5_FIELD_TABLE_305, buffer);
				PipeReader.releaseReadLock(input);
				buffer.position(0);
				for (int i = 0; i < 64; ++i) {
					table.table[i] = buffer.getInt();
				}
				header.quantizationTables.add(table);
			}
			else if (msgIdx == JPGSchema.MSG_MCUMESSAGE_6) {
				MCU mcu = new MCU();
				ByteBuffer yBuffer = ByteBuffer.allocate(64 * 2);
				ByteBuffer cbBuffer = ByteBuffer.allocate(64 * 2);
				ByteBuffer crBuffer = ByteBuffer.allocate(64 * 2);
				PipeReader.readBytes(input, JPGSchema.MSG_MCUMESSAGE_6_FIELD_Y_106, yBuffer);
				PipeReader.readBytes(input, JPGSchema.MSG_MCUMESSAGE_6_FIELD_CB_206, cbBuffer);
				PipeReader.readBytes(input, JPGSchema.MSG_MCUMESSAGE_6_FIELD_CR_306, crBuffer);
				PipeReader.releaseReadLock(input);
				yBuffer.position(0);
				cbBuffer.position(0);
				crBuffer.position(0);
				for (int i = 0; i < 64; ++i) {
					mcu.y[i] = yBuffer.getShort();
					mcu.cb[i] = cbBuffer.getShort();
					mcu.cr[i] = crBuffer.getShort();
				}
				
				dequantize(mcu, header);

				if (PipeWriter.tryWriteFragment(output, JPGSchema.MSG_MCUMESSAGE_6)) {
					ByteBuffer yBuffer2 = ByteBuffer.allocate(64 * 2);
					ByteBuffer cbBuffer2 = ByteBuffer.allocate(64 * 2);
					ByteBuffer crBuffer2 = ByteBuffer.allocate(64 * 2);
					for (int i = 0; i < 64; ++i) {
						yBuffer2.putShort(mcu.y[i]);
						cbBuffer2.putShort(mcu.cb[i]);
						crBuffer2.putShort(mcu.cr[i]);
					}
					yBuffer2.position(0);
					cbBuffer2.position(0);
					crBuffer2.position(0);
					PipeWriter.writeBytes(output, JPGSchema.MSG_MCUMESSAGE_6_FIELD_Y_106, yBuffer2);
					PipeWriter.writeBytes(output, JPGSchema.MSG_MCUMESSAGE_6_FIELD_CB_206, cbBuffer2);
					PipeWriter.writeBytes(output, JPGSchema.MSG_MCUMESSAGE_6_FIELD_CR_306, crBuffer2);
					PipeWriter.publishWrites(output);
				}
				else {
					System.err.println("Inverse Quantizer requesting shutdown");
					requestShutdown();
				}
			}
			else {
				System.err.println("Inverse Quantizer requesting shutdown");
				requestShutdown();
			}
		}
	}
}
