package com.ociweb.pronghorn.stage.math;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.pipe.token.TypeMask;
import com.ociweb.pronghorn.pipe.util.build.FROMValidation;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.math.BuildMatrixCompute.MatrixTypes;
import com.ociweb.pronghorn.stage.monitor.MonitorConsoleStage;
import com.ociweb.pronghorn.stage.monitor.PipeMonitorSchema;
import com.ociweb.pronghorn.stage.scheduling.FixedThreadsScheduler;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.StageScheduler;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;
import com.ociweb.pronghorn.stage.test.ConsoleJSONDumpStage;
import com.ociweb.pronghorn.stage.test.ConsoleSummaryStage;

public class MatrixComputeTest {

	
	@Test
	public void testColSplit() {
		
		int rows=10;
		int columns=6;
		MatrixSchema schema = BuildMatrixCompute.buildSchema(rows, columns, MatrixTypes.Integers);

		GraphManager gm = new GraphManager();		
		PipeConfig<MatrixSchema> matrixConfig = new PipeConfig<MatrixSchema>(schema, 2);
		
		ColumnSchema<MatrixSchema> cs = new ColumnSchema<MatrixSchema>(schema);
		PipeConfig<ColumnSchema<MatrixSchema>> columnConfig = new PipeConfig<ColumnSchema<MatrixSchema>>(cs);
		
		Pipe<ColumnSchema<MatrixSchema>>[] intputAsColumns = new Pipe[schema.getColumns()];
		
		Pipe<MatrixSchema> left = new Pipe<MatrixSchema>(matrixConfig); 
		
		
		new ColumnsRouteStage<>(gm, schema, left, intputAsColumns);
		int i = schema.getColumns();
		ByteArrayOutputStream[] targets = new ByteArrayOutputStream[i];
		PronghornStage[] watch = new PronghornStage[i];
		while (--i>=0) {
			targets[i] = new ByteArrayOutputStream();
			watch[i] =new ConsoleJSONDumpStage<>(gm, intputAsColumns[i]=new Pipe<ColumnSchema<MatrixSchema>>(columnConfig), new PrintStream(targets[i]));
		}
		
		
		//StageScheduler scheduler = new ThreadPerStageScheduler(gm);
		int targetThreadCount = 6;
		StageScheduler scheduler = new FixedThreadsScheduler(gm,targetThreadCount);
		
		scheduler.startup();	
				
		Pipe.addMsgIdx(left, schema.matrixId);		
		for(int c=0;c<schema.getRows();c++) {
			for(int r=0;r<schema.getColumns();r++) {
				Pipe.addIntValue(c, left);
			}
		}
		Pipe.confirmLowLevelWrite(left, Pipe.sizeOf(left, schema.matrixId));
		Pipe.publishWrites(left);		
		Pipe.publishEOF(left);
			
		i = schema.getColumns();
		while (--i>=0) {
			GraphManager.blockUntilStageBeginsShutdown(gm, watch[i], 500);//timeout in ms
		}
		
		scheduler.awaitTermination(2, TimeUnit.SECONDS);
		
		i = schema.getColumns();
		while (--i>=0) {
			String actualText = new String(targets[i].toByteArray());
			assertTrue(actualText.contains("{\"1\":0}"));
			assertTrue(actualText.contains("{\"5\":4}"));
			assertTrue(actualText.contains("{\"10\":9}"));
		}
		
	}

	@Test
	public void testCompute() {
		//speed
		//slow     Doubles  Longs
		//         Decimals
		//         Floats
		//fast     Integers
		
		
		
		MatrixTypes type = MatrixTypes.Integers;
		
		//TypeMask.Decimal;
		
		
		int leftRows=50;//100;
		int rightColumns=500;//100;//1000; //this also impacts the number of threads
				
		int leftColumns = 50;//100;//1000;
		int rightRows=leftColumns;		
		
		
		//walk leftRows , by rightCol for output
		
		
		MatrixSchema leftSchema = BuildMatrixCompute.buildSchema(leftRows, leftColumns, type);		
		MatrixSchema rightSchema = BuildMatrixCompute.buildSchema(rightRows, rightColumns, type);
		MatrixSchema resultSchema = BuildMatrixCompute.buildResultSchema(leftSchema, rightSchema);

		assertTrue(resultSchema.getRows()==leftRows);
		assertTrue(resultSchema.getColumns()==rightColumns);
		
		assertTrue(leftSchema.getRows()==leftRows);
		assertTrue(leftSchema.getColumns()==leftColumns);
		
		assertTrue(rightSchema.getRows()==rightRows);
		assertTrue(rightSchema.getColumns()==rightColumns);
				
		
		GraphManager gm = new GraphManager();
		
		GraphManager.addDefaultNota(gm, GraphManager.SCHEDULE_RATE, 500);
		
		Pipe<MatrixSchema> left = new Pipe<MatrixSchema>(new PipeConfig<MatrixSchema>(leftSchema, 1)); 
		Pipe<MatrixSchema> right = new Pipe<MatrixSchema>(new PipeConfig<MatrixSchema>(rightSchema, 1));
		Pipe<MatrixSchema> result = new Pipe<MatrixSchema>(new PipeConfig<MatrixSchema>(resultSchema, 2)); //NOTE: reqires 2 or JSON will not write out !!
		
		BuildMatrixCompute.buildGraph(gm, resultSchema, leftSchema, rightSchema, left, right, result);
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		
		ConsoleJSONDumpStage<MatrixSchema> watch = new ConsoleJSONDumpStage<>(gm, result, new PrintStream(baos));
		
	//	MonitorConsoleStage.attach(gm);
		
		int targetThreadCount = 12;
		StageScheduler scheduler = //new ThreadPerStageScheduler(gm);
				new FixedThreadsScheduler(gm, targetThreadCount);
		
		scheduler.startup();	
		
		int testSize = 100;//500;
		int k = testSize;
		while (--k>=0) {
			//System.out.println(k);
			
			while (!Pipe.hasRoomForWrite(left) || !Pipe.hasRoomForWrite(right)) {
				Thread.yield();
			}
			
			Pipe.addMsgIdx(left, resultSchema.matrixId);		
			for(int c=0;c<leftRows;c++) {
				for(int r=0;r<leftColumns;r++) {
					type.addValue(r, left);
				}
			}
			Pipe.confirmLowLevelWrite(left, Pipe.sizeOf(left, resultSchema.matrixId));
			Pipe.publishWrites(left);
			
			Pipe.addMsgIdx(right, resultSchema.matrixId);		
			for(int c=0;c<rightRows;c++) {
				for(int r=0;r<rightColumns;r++) {
					type.addValue(c, right);
				}
			}
			Pipe.confirmLowLevelWrite(right, Pipe.sizeOf(right, resultSchema.matrixId));
			Pipe.publishWrites(right);

		}
		
		Pipe.spinBlockForRoom(left, Pipe.EOF_SIZE);
		Pipe.spinBlockForRoom(right, Pipe.EOF_SIZE);
		Pipe.publishEOF(left);
		Pipe.publishEOF(right);
				
		GraphManager.blockUntilStageBeginsShutdown(gm, watch, 500);//timeout in ms
		

		scheduler.awaitTermination(2, TimeUnit.SECONDS);
				
		//System.out.println("len "+baos.toByteArray().length+"  "+new String(baos.toByteArray()));
		
		
	}
	
}
