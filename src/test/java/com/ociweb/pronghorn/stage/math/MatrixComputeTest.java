package com.ociweb.pronghorn.stage.math;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.concurrent.TimeUnit;

import org.junit.Ignore;
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
	public <M extends MatrixSchema<M>> void testRowsToColSplit() {
		
		int rows=10;
		int columns=6;
		MatrixSchema<M> schema = BuildMatrixCompute.buildSchema(rows, columns, MatrixTypes.Integers);
		ColumnSchema<M> cs = new ColumnSchema<M>(schema);
		RowSchema<M> rs = new RowSchema<M>(schema);

		GraphManager gm = new GraphManager();		
		PipeConfig<RowSchema<M>> rowConfig = new PipeConfig<RowSchema<M>>(rs, schema.getRows());		
		PipeConfig<ColumnSchema<M>> columnConfig = new PipeConfig<ColumnSchema<M>>(cs, 2);
		
		Pipe[] intputAsColumns = new Pipe[schema.getColumns()];
		
		Pipe<RowSchema<M>> inputRows = new Pipe<RowSchema<M>>(rowConfig); 

		int t = intputAsColumns.length;
		while (--t>=0) {
			intputAsColumns[t]=new Pipe<ColumnSchema<M>>(columnConfig);
		}
		
		
		new RowsToColumnRouteStage(gm, schema, inputRows, intputAsColumns);
		int i = schema.getColumns();
		ByteArrayOutputStream[] targets = new ByteArrayOutputStream[i];
		PronghornStage[] watch = new PronghornStage[i];
		while (--i>=0) {
			targets[i] = new ByteArrayOutputStream();
			watch[i] =new ConsoleJSONDumpStage<>(gm, intputAsColumns[i], new PrintStream(targets[i]));
		}
		
		
		
		
		//StageScheduler scheduler = new ThreadPerStageScheduler(gm);
		//int targetThreadCount = 6;
		StageScheduler scheduler = new ThreadPerStageScheduler(gm);
				//new FixedThreadsScheduler(gm,targetThreadCount); //TODO: do not enable until the thread grouping is balanced in FixedThreadScheduler
		
		scheduler.startup();	
				
		for(int c=0;c<schema.getRows();c++) {
			while (!Pipe.hasRoomForWrite(inputRows)) {
				Thread.yield();
			}
			
			Pipe.addMsgIdx(inputRows, schema.rowId);		
			for(int r=0;r<schema.getColumns();r++) {
				Pipe.addIntValue(c, inputRows);
			}
			Pipe.confirmLowLevelWrite(inputRows, Pipe.sizeOf(inputRows, schema.rowId));
			Pipe.publishWrites(inputRows);	
			
		}
		Pipe.publishEOF(inputRows);
			
		i = schema.getColumns();
		while (--i>=0) {
			GraphManager.blockUntilStageBeginsShutdown(gm, watch[i], 500);//timeout in ms
		}
		
		scheduler.awaitTermination(2, TimeUnit.SECONDS);
		
		i = schema.getColumns();
		while (--i>=0) {
			String actualText = new String(targets[i].toByteArray());
			
			//System.out.println(actualText);
			
			assertTrue(actualText.contains("{\"1\":0}"));
			assertTrue(actualText.contains("{\"5\":4}"));
			assertTrue(actualText.contains("{\"10\":9}"));
			
			assertTrue(actualText.indexOf("{\"1\":0}") > actualText.indexOf("{\"1\":1}"));
			
		}
		
	}

	@Test
	public <M extends MatrixSchema<M>> void testcolToRowsSplit() {
		
		int rows=10;
		int columns=6;
		MatrixSchema schema = BuildMatrixCompute.buildSchema(rows, columns, MatrixTypes.Integers);
		ColumnSchema<M> cs = new ColumnSchema<M>(schema);
		RowSchema<M> rs = new RowSchema<M>(schema);

		GraphManager gm = new GraphManager();		
		PipeConfig<RowSchema<M>> rowConfig = new PipeConfig<RowSchema<M>>(rs, schema.getRows());		
		PipeConfig<ColumnSchema<M>> columnConfig = new PipeConfig<ColumnSchema<M>>(cs, 2);
		
		Pipe<ColumnSchema<M>>[] columnsPipes = new Pipe[schema.getColumns()];
		
		int i = columnsPipes.length;
		while (--i>=0) {
			columnsPipes[i] = new Pipe<ColumnSchema<M>>(columnConfig);
		}
		
		Pipe<RowSchema<M>> rowsPipe = new Pipe<RowSchema<M>>(rowConfig); 
		
		
		new ColumnsToRowsStage(gm, schema, columnsPipes, rowsPipe);
		ByteArrayOutputStream capture = new ByteArrayOutputStream();
		ConsoleJSONDumpStage<RowSchema<M>> watch = new ConsoleJSONDumpStage<>(gm, rowsPipe, new PrintStream(capture));
		
		
		StageScheduler scheduler = new ThreadPerStageScheduler(gm);
		scheduler.startup();

		int c = columnsPipes.length;
		while (--c>=0) {
			
			int size = Pipe.addMsgIdx(columnsPipes[c], schema.columnId);
			int r = rows;
			while (--r >= 0) {
				Pipe.addIntValue(r, columnsPipes[c]);				
			}
			Pipe.confirmLowLevelWrite(columnsPipes[c], size);
			Pipe.publishWrites(columnsPipes[c]);
						
			
		}
		
		GraphManager.blockUntilStageBeginsShutdown(gm, watch, 500);
		
		String actualText = new String(capture.toByteArray());
		//System.out.println(actualText);
		
		assertTrue(actualText.contains("{\"1\":0}"));
		assertTrue(actualText.contains("{\"5\":4}"));
		assertTrue(actualText.contains("{\"6\":9}"));
		
		assertTrue(actualText.indexOf("{\"1\":0}") > actualText.indexOf("{\"1\":1}"));
		
	}
	
	
	@Ignore
	public <M extends MatrixSchema<M>> void testCompute() {
		//speed
		//slow     Doubles  Longs    6.15 5.8      7.024  7.18
		//         Decimals          5.9           9.40 - 13
		//         Floats            6.06           6.26
		//fast     Integers          5.80           5.95
		
		//TODO: convert all to Decimals for unit test check.
	
		MatrixTypes type = MatrixTypes.Integers;//Decimals;//Integers; //2, 3328335 longs/ints/doubles   [0,332833152] floats
		
		//TypeMask.Decimal;
		
		//targeting 7680 × 4320
		
		int leftRows=1080;
		int rightColumns=1920;
				
		int leftColumns = 1920;
		int rightRows=leftColumns;		
		
		
		//walk leftRows , by rightCol for output
		//5x2
		//2x3
		
		//TODO: these 3 must be removed since they are not "real" schemas but just hold the type and matrix size.
		MatrixSchema<M> leftSchema = BuildMatrixCompute.buildSchema(leftRows, leftColumns, type);		
		MatrixSchema<M> rightSchema = BuildMatrixCompute.buildSchema(rightRows, rightColumns, type);
		MatrixSchema<M> resultSchema = BuildMatrixCompute.buildResultSchema(leftSchema, rightSchema);
		
		
		RowSchema<M> leftRowSchema = new RowSchema<M>(leftSchema);
		RowSchema<M> rightRowSchema = new RowSchema<M>(rightSchema);				
		RowSchema<M> rowResultSchema = new RowSchema<M>(resultSchema);		
		
		
		DecimalSchema<M> result2Schema = new DecimalSchema<M>(resultSchema);

		assertTrue(resultSchema.getRows()==leftRows);
		assertTrue(resultSchema.getColumns()==rightColumns);
		
		assertTrue(leftSchema.getRows()==leftRows);
		assertTrue(leftSchema.getColumns()==leftColumns);
		
		assertTrue(rightSchema.getRows()==rightRows);
		assertTrue(rightSchema.getColumns()==rightColumns);
				
		
		GraphManager gm = new GraphManager();
		
		GraphManager.addDefaultNota(gm, GraphManager.SCHEDULE_RATE, 500);
		
		Pipe<RowSchema<M>> left = new Pipe<RowSchema<M>>(new PipeConfig<RowSchema<M>>(leftRowSchema, leftRows)); 
		Pipe<RowSchema<M>> right = new Pipe<RowSchema<M>>(new PipeConfig<RowSchema<M>>(rightRowSchema, rightRows));
		
		Pipe<RowSchema<M>> result = new Pipe<RowSchema<M>>(new PipeConfig<RowSchema<M>>(rowResultSchema, resultSchema.getRows())); //NOTE: reqires 2 or JSON will not write out !!
	//	Pipe<DecimalSchema<MatrixSchema>> result2 = new Pipe<DecimalSchema<MatrixSchema>>(new PipeConfig<DecimalSchema<MatrixSchema>>(result2Schema, resultSchema.getRows())); //NOTE: reqires 2 or JSON will not write out !!
		
		
		int targetThreadCount = 12;
		Pipe<ColumnSchema<M>>[] colResults = BuildMatrixCompute.buildGraph(gm, resultSchema, leftSchema, rightSchema, left, right, targetThreadCount-2);
		
		ColumnsToRowsStage<M> ctr = new ColumnsToRowsStage(gm, resultSchema, colResults, result);
		
		
		//ByteArrayOutputStream baos = new ByteArrayOutputStream();
		
		
		//ConvertToDecimalStage<MatrixSchema> convert = new ConvertToDecimalStage<MatrixSchema>(gm, resultSchema, result, result2);
		
		//ConsoleJSONDumpStage<?> watch = new ConsoleJSONDumpStage<>(gm, result , new PrintStream(baos));
		ConsoleSummaryStage<RowSchema<M>> watch = new ConsoleSummaryStage<>(gm, result);
		
		//gm.exportGraphDotFile();
		
		//MonitorConsoleStage.attach(gm);
		
		StageScheduler scheduler = new ThreadPerStageScheduler(gm);
			                     //new FixedThreadsScheduler(gm, targetThreadCount);
		
		scheduler.startup();	
	
		int testSize = 10;
		int k = testSize;
	
		while (--k>=0) {
			//System.out.println(k);
			for(int c=0;c<leftRows;c++) {
				while (!Pipe.hasRoomForWrite(left)) {
					Thread.yield();
				}
				Pipe.addMsgIdx(left, resultSchema.rowId);		
					for(int r=0;r<leftColumns;r++) {
						type.addValue(r, left);
					}
				Pipe.confirmLowLevelWrite(left, Pipe.sizeOf(left, resultSchema.rowId));
				Pipe.publishWrites(left);
			}
			
			for(int c=0;c<rightRows;c++) {
				while (!Pipe.hasRoomForWrite(right)) {
					Thread.yield();
				}
				Pipe.addMsgIdx(right, resultSchema.rowId);		
					for(int r=0;r<rightColumns;r++) {
						type.addValue(c, right);
					}
				Pipe.confirmLowLevelWrite(right, Pipe.sizeOf(right, resultSchema.rowId));
				Pipe.publishWrites(right);
			}

		}
		
		
		if (k<0) {
			while (!Pipe.hasRoomForWrite(left, Pipe.EOF_SIZE)) {
			    Pipe.spinWork(left);
			}
			while (!Pipe.hasRoomForWrite(right, Pipe.EOF_SIZE)) {
			    Pipe.spinWork(right);
			}
			Pipe.publishEOF(left);
			Pipe.publishEOF(right);
		}
		GraphManager.blockUntilStageBeginsShutdown(gm, watch, 500);//timeout in ms
		

		scheduler.awaitTermination(20, TimeUnit.SECONDS);
				
	//	System.out.println("len "+baos.toByteArray().length+"  "+new String(baos.toByteArray()));
		
		
	}
	
	@Test
	public <M extends MatrixSchema<M>> void testComputeExample() {
		//speed
		//slow     Doubles  Longs    6.15 5.8      7.024  7.18
		//         Decimals          5.9           9.40 - 13
		//         Floats            6.06           6.26
		//fast     Integers          5.80           5.95
		
		//TODO: convert all to Decimals for unit test check.
	
		MatrixTypes type = MatrixTypes.Integers;//Decimals;//Integers; //2, 3328335 longs/ints/doubles   [0,332833152] floats
		
		//TypeMask.Decimal;
		
		
		int leftRows=5;
		int rightColumns=3;
				
		int leftColumns = 2;
		int rightRows=leftColumns;		
		
		
		//walk leftRows , by rightCol for output
		//5x2
		//2x3
		
		int[][] leftTest = new int[][] {
			{1,2},
			{4,4},
			{7,7},
			{3,2},
			{1,1},
		}; 
		
		int[][] rightTest = new int[][] {
			{1,2,3},
			{4,4,4}
		}; 
		

		
		
		
		MatrixSchema leftSchema = BuildMatrixCompute.buildSchema(leftRows, leftColumns, type);		
		RowSchema<M> leftRowSchema = new RowSchema<M>(leftSchema);
		
		MatrixSchema rightSchema = BuildMatrixCompute.buildSchema(rightRows, rightColumns, type);
		RowSchema<M> rightRowSchema = new RowSchema<M>(rightSchema);
				
		MatrixSchema resultSchema = BuildMatrixCompute.buildResultSchema(leftSchema, rightSchema);
		RowSchema<M> rowResultSchema = new RowSchema<M>(resultSchema);		
		
		
		DecimalSchema<M> result2Schema = new DecimalSchema<M>(resultSchema);

		assertTrue(resultSchema.getRows()==leftRows);
		assertTrue(resultSchema.getColumns()==rightColumns);
		
		assertTrue(leftSchema.getRows()==leftRows);
		assertTrue(leftSchema.getColumns()==leftColumns);
		
		assertTrue(rightSchema.getRows()==rightRows);
		assertTrue(rightSchema.getColumns()==rightColumns);
				
		
		GraphManager gm = new GraphManager();
		
		GraphManager.addDefaultNota(gm, GraphManager.SCHEDULE_RATE, 500);
		
		Pipe<RowSchema<M>> left = new Pipe<RowSchema<M>>(new PipeConfig<RowSchema<M>>(leftRowSchema, leftRows)); 
		Pipe<RowSchema<M>> right = new Pipe<RowSchema<M>>(new PipeConfig<RowSchema<M>>(rightRowSchema, rightRows));
		
		Pipe<RowSchema<M>> result = new Pipe<RowSchema<M>>(new PipeConfig<RowSchema<M>>(rowResultSchema, resultSchema.getRows())); //NOTE: reqires 2 or JSON will not write out !!
		Pipe<DecimalSchema<M>> result2 = new Pipe<DecimalSchema<M>>(new PipeConfig<DecimalSchema<M>>(result2Schema, resultSchema.getRows())); //NOTE: reqires 2 or JSON will not write out !!
		
		
		int targetThreadCount = 12;
		Pipe<ColumnSchema<M>>[] colResults = BuildMatrixCompute.buildGraph(gm, resultSchema, leftSchema, rightSchema, left, right, targetThreadCount-2);
		
		ColumnsToRowsStage<M> ctr = new ColumnsToRowsStage(gm, resultSchema, colResults, result);
		
		
		ConvertToDecimalStage<M> watch = new ConvertToDecimalStage(gm, rowResultSchema, result, result2);
		
		result2.initBuffers(); //required for us to jump in on this thread and grab the data.

		
		StageScheduler scheduler = new ThreadPerStageScheduler(gm);
		
		scheduler.startup();	

		for(int c=0;c<leftRows;c++) {
			while (!Pipe.hasRoomForWrite(left)) {
				Thread.yield();
			}
			Pipe.addMsgIdx(left, resultSchema.rowId);	
			for(int r=0;r<leftColumns;r++) {
				type.addValue(leftTest[c][r], left);
			}
			Pipe.confirmLowLevelWrite(left, Pipe.sizeOf(left, resultSchema.rowId));
			Pipe.publishWrites(left);
		}
		
		for(int c=0;c<rightRows;c++) {
			while (!Pipe.hasRoomForWrite(right)) {
				Thread.yield();
			}
			Pipe.addMsgIdx(right, resultSchema.rowId);		
			for(int r=0;r<rightColumns;r++) {
				type.addValue(rightTest[c][r], right);
			}
			Pipe.confirmLowLevelWrite(right, Pipe.sizeOf(right, resultSchema.rowId));
			Pipe.publishWrites(right);
		}

		
		while (!Pipe.hasRoomForWrite(left, Pipe.EOF_SIZE)) {
		    Pipe.spinWork(left);
		}
		while (!Pipe.hasRoomForWrite(right, Pipe.EOF_SIZE)) {
		    Pipe.spinWork(right);
		}
		Pipe.publishEOF(left);
		Pipe.publishEOF(right);
				
		GraphManager.blockUntilStageBeginsShutdown(gm, watch, 500);//timeout in ms
		

		scheduler.awaitTermination(2000, TimeUnit.SECONDS);
		
		int[][] expectedAnswer = new int[][] {
			{9,10,11},
			{20,24,28},
			{35,42,49},
			{11,14,17},
			{5,6,7},
			
		}; 
		
	//	String actual = new String(baos.toByteArray());
		
		for(int r=0;r<5;r++) {
			assertTrue(result2.hasContentToRead(result2));
	
			int id = Pipe.takeMsgIdx(result2);
			for(int c=0;c<3;c++) {
					
					int exp = Pipe.takeInt(result2);					
					long man = Pipe.takeLong(result2);
					
					long value = (long)Math.rint(man*Math.pow(10, exp));
					
					assertEquals(expectedAnswer[r][c], value);
			}
			
			Pipe.confirmLowLevelRead(result2, Pipe.sizeOf(result2, id));
			Pipe.releaseReadLock(result2);
		}

		
	}
	
}
