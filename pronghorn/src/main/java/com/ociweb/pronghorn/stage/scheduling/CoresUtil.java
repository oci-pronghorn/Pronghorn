package com.ociweb.pronghorn.stage.scheduling;

public class CoresUtil {

	private static int custom = -2;
	
	public static int availableProcessors() {
		
		if (-2==custom) {
			String value= System.getProperty("pronghorn.processors");
			if (value!=null) {
				custom = Integer.parseInt(value);
				System.out.println("DETECTED PROPERTY pronghorn.processors "+custom+" this new value of processors will be used.");
			}
			if (custom<=0) {
				custom=0;
			}
		}
		int result = custom>0 ? custom : Runtime.getRuntime().availableProcessors();

		return result;
	}
	
}
