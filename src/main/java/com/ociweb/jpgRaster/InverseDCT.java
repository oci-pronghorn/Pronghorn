package com.ociweb.jpgRaster;

import java.util.ArrayList;

import com.ociweb.jpgRaster.JPG.MCU;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class InverseDCT extends PronghornStage {

	private final Pipe<JPGSchema> input;
	private final Pipe<JPGSchema> output;
	
	
	protected InverseDCT(GraphManager graphManager, Pipe<JPGSchema> input, Pipe<JPGSchema> output) {
		super(graphManager, input, output);
		this.input = input;
		this.output = output;
	}
	
	private static double[] idctMap = new double[64];
	
	// prepare idctMap
	static {
		for (int u = 0; u < 8; ++u) {
			double c = 1.0f;
			if (u == 0) {
				c = 1 / Math.sqrt(2.0);
			}
			for (int x = 0; x < 8; ++x) {
				idctMap[u * 8 + x] = c * Math.cos((2.0 * x + 1.0) * u * Math.PI / 16.0);
			}
		}
	}
	
	private static short[] MCUInverseDCT(short[] mcu) {
		/*System.out.print("Before Inverse DCT:");
		for (int i = 0; i < 8; ++i) {
			for (int j = 0; j < 8; ++j) {
				if (j % 8 == 0) {
					System.out.println();
				}
				System.out.print(mcu[i * 8 + j] + " ");
			}
		}
		System.out.println();*/
		
		short[] result = new short[64];
		
		for (int y = 0; y < 8; ++y) {
			for (int x = 0; x < 8; ++x) {
				double sum = 0.0;
				for (int i = 0; i < 8; ++i) {
					for (int j = 0; j < 8; ++j) {
						sum += mcu[i * 8 + j] *
							   idctMap[j * 8 + x] *
							   idctMap[i * 8 + y];
					}
				}
				sum /= 4.0;
				result[y * 8 + x] = (short)sum;
			}
		}
		
		/*System.out.print("After Inverse DCT:");
		for (int i = 0; i < 8; ++i) {
			for (int j = 0; j < 8; ++j) {
				if (j % 8 == 0) {
					System.out.println();
				}
				System.out.print(result[i * 8 + j] + " ");
			}
		}
		System.out.println();*/
		
		return result;
	}
	
	public static void inverseDCT(ArrayList<MCU> mcus) {
		for (int i = 0; i < mcus.size(); ++i) {
			mcus.get(i).y =  MCUInverseDCT(mcus.get(i).y);
			mcus.get(i).cb = MCUInverseDCT(mcus.get(i).cb);
			mcus.get(i).cr = MCUInverseDCT(mcus.get(i).cr);
		}
		return;
	}

	@Override
	public void run() {
		
	}
	
	/*public static void main(String[] args) {
		short[] mcu = new short[] {
				-252, -36,  -5, -6, 15, -4, 6, 0,
				  55,  84, -14,  7,  0,  0, 0, 0,
				  20,   0, -18, -9,  0,  0, 0, 0,
				 -24,  32,   0,  0,  0,  0, 0, 0,
				 22,  -22,   0,  0,  0,  0, 0, 0,
				  0,    0,   0,  0,  0,  0, 0, 0,
				  0,    0,   0,  0,  0,  0, 0, 0,
				  0,    0,   0,  0,  0,  0, 0, 0
		};
		short[] result = MCUInverseDCT(mcu);
		for (int i = 0; i < 8; i++) {
			for (int j = 0; j < 8; j++) {
				System.out.print(String.format("%04d ", result[i * 8 + j]));
			}
			System.out.println();
		}
	}*/
}
