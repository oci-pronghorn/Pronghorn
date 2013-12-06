package com.ociweb.jfast.stream;

import com.google.caliper.runner.CaliperMain;

public class RunCaliper {

	public static void main(String[] args) {
		//  -XX:+UseNUMA
		args = new String[]{"-r","HomogeniousRecordWriteReadBenchmark",
				             //"-t","3",
				             "-Cinstrument.micro.options.warmup=1s"};
		
		//-h
		//-C 5ms
		//--verbose
		
		CaliperMain.main(HomogeniousRecordWriteReadBenchmark.class, args); 

	}

}
