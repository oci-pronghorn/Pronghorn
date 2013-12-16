package com.ociweb.jfast.stream;

import com.google.caliper.runner.CaliperMain;

public class RunCaliper {

	public static void main(String[] args) {

//	run(HomogeniousRecordWriteReadDecimalBenchmark.class); 
	//	run(HomogeniousRecordWriteReadIntegerBenchmark.class); 
    	run(HomogeniousRecordWriteReadLongBenchmark.class); 
	}

	private static void run(Class clazz) {
		String[] args;
		//  -XX:+UseNUMA
		args = new String[]{"-r",clazz.getSimpleName(),
				             "-Cinstrument.micro.options.warmup=1s"};
		
		//-h
		//-C 5ms
		//--verbose
		
		CaliperMain.main(clazz, args);
	}

}
