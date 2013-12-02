package com.ociweb.jfast.stream;

import com.google.caliper.runner.CaliperMain;

public class RunCaliper {

	public static void main(String[] args) {
		//  -XX:+UseNUMA
		args = new String[]{"-r","HomogeniousRecordWriteReadBenchmark",
				             "-C","=5ms"};
		
		//-h
		//-C 5ms
		//--verbose
		
		CaliperMain.main(HomogeniousRecordWriteReadBenchmark.class, args); 

	}

}
