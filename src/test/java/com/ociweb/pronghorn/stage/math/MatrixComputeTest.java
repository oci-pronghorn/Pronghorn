package com.ociweb.pronghorn.stage.math;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.concurrent.TimeUnit;
import java.util.Random;

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
	public void testRowsToColSplit() {
		
		int rows=10;
		int columns=6;
		MatrixSchema schema = BuildMatrixCompute.buildSchema(rows, columns, MatrixTypes.Integers);
		ColumnSchema<MatrixSchema> cs = new ColumnSchema<MatrixSchema>(schema);
		RowSchema<MatrixSchema> rs = new RowSchema<MatrixSchema>(schema);

		GraphManager gm = new GraphManager();		
		PipeConfig<RowSchema<MatrixSchema>> rowConfig = new PipeConfig<RowSchema<MatrixSchema>>(rs, schema.getRows());		
		PipeConfig<ColumnSchema<MatrixSchema>> columnConfig = new PipeConfig<ColumnSchema<MatrixSchema>>(cs, 2);
		
		Pipe<ColumnSchema<MatrixSchema>>[] intputAsColumns = new Pipe[schema.getColumns()];
		
		Pipe<RowSchema<MatrixSchema>> inputRows = new Pipe<RowSchema<MatrixSchema>>(rowConfig); 

		int t = intputAsColumns.length;
		while (--t>=0) {
			intputAsColumns[t]=new Pipe<ColumnSchema<MatrixSchema>>(columnConfig);
		}
		
		
		new RowsToColumnRouteStage<>(gm, schema, inputRows, intputAsColumns);
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
	public void testcolToRowsSplit() {
		
		int rows=10;
		int columns=6;
		MatrixSchema schema = BuildMatrixCompute.buildSchema(rows, columns, MatrixTypes.Integers);
		ColumnSchema<MatrixSchema> cs = new ColumnSchema<MatrixSchema>(schema);
		RowSchema<MatrixSchema> rs = new RowSchema<MatrixSchema>(schema);

		GraphManager gm = new GraphManager();		
		PipeConfig<RowSchema<MatrixSchema>> rowConfig = new PipeConfig<RowSchema<MatrixSchema>>(rs, schema.getRows());		
		PipeConfig<ColumnSchema<MatrixSchema>> columnConfig = new PipeConfig<ColumnSchema<MatrixSchema>>(cs, 2);
		
		Pipe<ColumnSchema<MatrixSchema>>[] columnsPipes = new Pipe[schema.getColumns()];
		
		int i = columnsPipes.length;
		while (--i>=0) {
			columnsPipes[i] = new Pipe<ColumnSchema<MatrixSchema>>(columnConfig);
		}
		
		Pipe<RowSchema<MatrixSchema>> rowsPipe = new Pipe<RowSchema<MatrixSchema>>(rowConfig); 
		
		
		new ColumnsToRowsStage(gm, schema, columnsPipes, rowsPipe);
		ByteArrayOutputStream capture = new ByteArrayOutputStream();
		ConsoleJSONDumpStage<RowSchema<MatrixSchema>> watch = new ConsoleJSONDumpStage<>(gm, rowsPipe, new PrintStream(capture));
		
		
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
	
	@Test
	public void testCompute() {
		//speed
		//slow     Doubles  Longs    6.15 5.8      7.024  7.18
		//         Decimals          5.9           9.40 - 13
		//         Floats            6.06           6.26
		//fast     Integers          5.80           5.95
		
		//TODO: convert all to Decimals for unit test check.
	
		MatrixTypes type = MatrixTypes.Integers;//Decimals;//Integers; //2, 3328335 longs/ints/doubles   [0,332833152] floats
		
		//TypeMask.Decimal;
		
		
		int leftRows=10;
		int rightColumns=1024;
				
		int leftColumns = 1024;
		int rightRows=leftColumns;		
		
		
		//walk leftRows , by rightCol for output
		//5x2
		//2x3
		
		//TODO: these 3 must be removed since they are not "real" schemas but just hold the type and matrix size.
		MatrixSchema leftSchema = BuildMatrixCompute.buildSchema(leftRows, leftColumns, type);		
		MatrixSchema rightSchema = BuildMatrixCompute.buildSchema(rightRows, rightColumns, type);
		MatrixSchema resultSchema = BuildMatrixCompute.buildResultSchema(leftSchema, rightSchema);
		
		
		RowSchema<MatrixSchema> leftRowSchema = new RowSchema<MatrixSchema>(leftSchema);
		RowSchema<MatrixSchema> rightRowSchema = new RowSchema<MatrixSchema>(rightSchema);				
		RowSchema<MatrixSchema> rowResultSchema = new RowSchema<MatrixSchema>(resultSchema);		
		
		
		DecimalSchema result2Schema = new DecimalSchema<MatrixSchema>(resultSchema);

		assertTrue(resultSchema.getRows()==leftRows);
		assertTrue(resultSchema.getColumns()==rightColumns);
		
		assertTrue(leftSchema.getRows()==leftRows);
		assertTrue(leftSchema.getColumns()==leftColumns);
		
		assertTrue(rightSchema.getRows()==rightRows);
		assertTrue(rightSchema.getColumns()==rightColumns);
				
		
		GraphManager gm = new GraphManager();
		
		GraphManager.addDefaultNota(gm, GraphManager.SCHEDULE_RATE, 500);
		
		Pipe<RowSchema<MatrixSchema>> left = new Pipe<RowSchema<MatrixSchema>>(new PipeConfig<RowSchema<MatrixSchema>>(leftRowSchema, leftRows)); 
		Pipe<RowSchema<MatrixSchema>> right = new Pipe<RowSchema<MatrixSchema>>(new PipeConfig<RowSchema<MatrixSchema>>(rightRowSchema, rightRows));
		
		Pipe<RowSchema<MatrixSchema>> result = new Pipe<RowSchema<MatrixSchema>>(new PipeConfig<RowSchema<MatrixSchema>>(rowResultSchema, resultSchema.getRows())); //NOTE: reqires 2 or JSON will not write out !!
	//	Pipe<DecimalSchema<MatrixSchema>> result2 = new Pipe<DecimalSchema<MatrixSchema>>(new PipeConfig<DecimalSchema<MatrixSchema>>(result2Schema, resultSchema.getRows())); //NOTE: reqires 2 or JSON will not write out !!
		
		
		int targetThreadCount = 12;
		Pipe<ColumnSchema<MatrixSchema>>[] colResults = BuildMatrixCompute.buildGraph(gm, resultSchema, leftSchema, rightSchema, left, right, targetThreadCount-2);
		
		ColumnsToRowsStage<MatrixSchema> ctr = new ColumnsToRowsStage<MatrixSchema>(gm, resultSchema, colResults, result);
		
		
		//ByteArrayOutputStream baos = new ByteArrayOutputStream();
		
		
		//ConvertToDecimalStage<MatrixSchema> convert = new ConvertToDecimalStage<MatrixSchema>(gm, resultSchema, result, result2);
		
		//ConsoleJSONDumpStage<?> watch = new ConsoleJSONDumpStage<>(gm, result , new PrintStream(baos));
		ConsoleSummaryStage<RowSchema<MatrixSchema>> watch = new ConsoleSummaryStage<>(gm, result);
		
		//gm.exportGraphDotFile();
		
		//MonitorConsoleStage.attach(gm);
		
		StageScheduler scheduler = new ThreadPerStageScheduler(gm);
			                     //new FixedThreadsScheduler(gm, targetThreadCount);
		
		scheduler.startup();	
		
		int testSize = 1;
		int k = testSize;
		long timeout = 0;
		while (--k>=0) {
			timeout = System.currentTimeMillis()+5000;
			//System.out.println(k);
			for(int c=0;c<leftRows;c++) {
				while (!Pipe.hasRoomForWrite(left)) {
					Thread.yield();
					if (System.currentTimeMillis()>timeout) {
						scheduler.shutdown();
						scheduler.awaitTermination(20, TimeUnit.SECONDS);
						fail();
						return;
					}
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
					if (System.currentTimeMillis()>timeout) {
						scheduler.shutdown();
						scheduler.awaitTermination(20, TimeUnit.SECONDS);
						fail();
						return;
					}
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
			Pipe.spinBlockForRoom(left, Pipe.EOF_SIZE);
			Pipe.spinBlockForRoom(right, Pipe.EOF_SIZE);
			Pipe.publishEOF(left);
			Pipe.publishEOF(right);
		}
		GraphManager.blockUntilStageBeginsShutdown(gm, watch, 500);//timeout in ms
		

		scheduler.awaitTermination(20, TimeUnit.SECONDS);
				
	//	System.out.println("len "+baos.toByteArray().length+"  "+new String(baos.toByteArray()));
		
		
	}

	@Test
	public void testComputeExample() {
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
		RowSchema<MatrixSchema> leftRowSchema = new RowSchema<MatrixSchema>(leftSchema);
		
		MatrixSchema rightSchema = BuildMatrixCompute.buildSchema(rightRows, rightColumns, type);
		RowSchema<MatrixSchema> rightRowSchema = new RowSchema<MatrixSchema>(rightSchema);
				
		MatrixSchema resultSchema = BuildMatrixCompute.buildResultSchema(leftSchema, rightSchema);
		RowSchema<MatrixSchema> rowResultSchema = new RowSchema<MatrixSchema>(resultSchema);		
		
		
		DecimalSchema result2Schema = new DecimalSchema<MatrixSchema>(resultSchema);

		assertTrue(resultSchema.getRows()==leftRows);
		assertTrue(resultSchema.getColumns()==rightColumns);
		
		assertTrue(leftSchema.getRows()==leftRows);
		assertTrue(leftSchema.getColumns()==leftColumns);
		
		assertTrue(rightSchema.getRows()==rightRows);
		assertTrue(rightSchema.getColumns()==rightColumns);
				
		
		GraphManager gm = new GraphManager();
		
		GraphManager.addDefaultNota(gm, GraphManager.SCHEDULE_RATE, 500);
		
		Pipe<RowSchema<MatrixSchema>> left = new Pipe<RowSchema<MatrixSchema>>(new PipeConfig<RowSchema<MatrixSchema>>(leftRowSchema, leftRows)); 
		Pipe<RowSchema<MatrixSchema>> right = new Pipe<RowSchema<MatrixSchema>>(new PipeConfig<RowSchema<MatrixSchema>>(rightRowSchema, rightRows));
		
		Pipe<RowSchema<MatrixSchema>> result = new Pipe<RowSchema<MatrixSchema>>(new PipeConfig<RowSchema<MatrixSchema>>(rowResultSchema, resultSchema.getRows())); //NOTE: reqires 2 or JSON will not write out !!
		Pipe<DecimalSchema<MatrixSchema>> result2 = new Pipe<DecimalSchema<MatrixSchema>>(new PipeConfig<DecimalSchema<MatrixSchema>>(result2Schema, resultSchema.getRows())); //NOTE: reqires 2 or JSON will not write out !!
		
		
		int targetThreadCount = 12;
		Pipe<ColumnSchema<MatrixSchema>>[] colResults = BuildMatrixCompute.buildGraph(gm, resultSchema, leftSchema, rightSchema, left, right, targetThreadCount-2);
		
		ColumnsToRowsStage<MatrixSchema> ctr = new ColumnsToRowsStage<MatrixSchema>(gm, resultSchema, colResults, result);
		
		
		ConvertToDecimalStage<MatrixSchema> watch = new ConvertToDecimalStage<MatrixSchema>(gm, rowResultSchema, result, result2);
		
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

		
		Pipe.spinBlockForRoom(left, Pipe.EOF_SIZE);
		Pipe.spinBlockForRoom(right, Pipe.EOF_SIZE);
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
	
        @Ignore
	@Test
	public void testComputeExampleNativeInteger() {
		MatrixTypes type = MatrixTypes.Integers;//Decimals;//Integers; //2, 3328335 longs/ints/doubles   [0,332833152] floats
		
		//TypeMask.Decimal;
		
		
		int leftRows = 10;
		int leftColumns = 1048;

		int rightColumns = 1048;				
		int rightRows = leftColumns;		
		
		Random rand = new Random();
		
		int[][] leftTest = new int[leftRows][leftColumns];
		for (int i = 0; i < leftRows; ++i) {
		    for (int j = 0; j < leftColumns; ++j) {
			leftTest[i][j] = rand.nextInt(100);
		    }
		}
		
		int[][] rightTest = new int[rightRows][rightColumns];
		for (int i = 0; i < rightRows; ++i) {
		    for (int j = 0; j < rightColumns; ++j) {
			rightTest[i][j] = rand.nextInt(100);
		    }
		}
								
		MatrixSchema leftSchema = BuildMatrixCompute.buildSchema(leftRows, leftColumns, type);		
		RowSchema<MatrixSchema> leftRowSchema = new RowSchema<MatrixSchema>(leftSchema);
		
		MatrixSchema rightSchema = BuildMatrixCompute.buildSchema(rightRows, rightColumns, type);
		RowSchema<MatrixSchema> rightRowSchema = new RowSchema<MatrixSchema>(rightSchema);
				
		MatrixSchema resultSchema = BuildMatrixCompute.buildResultSchema(leftSchema, rightSchema);
		RowSchema<MatrixSchema> rowResultSchema = new RowSchema<MatrixSchema>(resultSchema);		
		
		
		DecimalSchema result2Schema = new DecimalSchema<MatrixSchema>(resultSchema);

		assertTrue(resultSchema.getRows()==leftRows);
		assertTrue(resultSchema.getColumns()==rightColumns);
		
		assertTrue(leftSchema.getRows()==leftRows);
		assertTrue(leftSchema.getColumns()==leftColumns);
		
		assertTrue(rightSchema.getRows()==rightRows);
		assertTrue(rightSchema.getColumns()==rightColumns);
				
		
		GraphManager gm = new GraphManager();
		
		GraphManager.addDefaultNota(gm, GraphManager.SCHEDULE_RATE, 500);
		
		Pipe<RowSchema<MatrixSchema>> left = new Pipe<RowSchema<MatrixSchema>>(new PipeConfig<RowSchema<MatrixSchema>>(leftRowSchema, leftRows)); 
		Pipe<RowSchema<MatrixSchema>> right = new Pipe<RowSchema<MatrixSchema>>(new PipeConfig<RowSchema<MatrixSchema>>(rightRowSchema, rightRows));
		
		Pipe<RowSchema<MatrixSchema>> result = new Pipe<RowSchema<MatrixSchema>>(new PipeConfig<RowSchema<MatrixSchema>>(rowResultSchema, resultSchema.getRows())); //NOTE: reqires 2 or JSON will not write out !!
		Pipe<DecimalSchema<MatrixSchema>> result2 = new Pipe<DecimalSchema<MatrixSchema>>(new PipeConfig<DecimalSchema<MatrixSchema>>(result2Schema, resultSchema.getRows())); //NOTE: reqires 2 or JSON will not write out !!
		
		
		int targetThreadCount = 12;
		Pipe<ColumnSchema<MatrixSchema>>[] colResults = BuildMatrixCompute.buildGraph(gm, resultSchema, leftSchema, rightSchema, left, right, targetThreadCount-2);
		
		ColumnsToRowsStage<MatrixSchema> ctr = new ColumnsToRowsStage<MatrixSchema>(gm, resultSchema, colResults, result);
		
		
		ConvertToDecimalStage<MatrixSchema> watch = new ConvertToDecimalStage<MatrixSchema>(gm, rowResultSchema, result, result2);
		
		result2.initBuffers(); //required for us to jump in on this thread and grab the data.

		
		StageScheduler scheduler = new ThreadPerStageScheduler(gm);
		
		scheduler.startup();	

		long timeout = System.currentTimeMillis()+5000;
		for(int c=0;c<leftRows;c++) {
			while (!Pipe.hasRoomForWrite(left)) {
				Thread.yield();
				if (System.currentTimeMillis()>timeout) {
				    scheduler.shutdown();
				    scheduler.awaitTermination(20, TimeUnit.SECONDS);
				    fail();
				    return;
				}
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
				if (System.currentTimeMillis()>timeout) {
				    scheduler.shutdown();
				    scheduler.awaitTermination(20, TimeUnit.SECONDS);
				    fail();
				    return;
				}
			}
			Pipe.addMsgIdx(right, resultSchema.rowId);		
			for(int r=0;r<rightColumns;r++) {
				type.addValue(rightTest[c][r], right);
			}
			Pipe.confirmLowLevelWrite(right, Pipe.sizeOf(right, resultSchema.rowId));
			Pipe.publishWrites(right);
		}
		
		Pipe.spinBlockForRoom(left, Pipe.EOF_SIZE);
		Pipe.spinBlockForRoom(right, Pipe.EOF_SIZE);
		Pipe.publishEOF(left);
		Pipe.publishEOF(right);
				
		GraphManager.blockUntilStageBeginsShutdown(gm, watch, 500);//timeout in ms
		

		scheduler.awaitTermination(2000, TimeUnit.SECONDS);
		
		int[][] expectedAnswer = new int[leftRows][rightColumns];
		for (int i = 0; i < leftRows; ++i) {
		    for (int j = 0; j < rightColumns; ++j) {
			for (int k = 0; k < leftColumns; ++k) {
			    expectedAnswer[i][j] += leftTest[i][k] * rightTest[k][j];
			}
		    }
		}
				
	//	String actual = new String(baos.toByteArray());
		
		for(int r=0;r<leftRows;r++) {
			assertTrue(result2.hasContentToRead(result2));
	
			int id = Pipe.takeMsgIdx(result2);
			for(int c=0;c<rightColumns;c++) {
					
					int exp = Pipe.takeValue(result2);					
					long man = Pipe.takeLong(result2);
					
					long value = (long)Math.rint(man*Math.pow(10, exp));
					
					assertEquals(expectedAnswer[r][c], value);
			}
			
			Pipe.confirmLowLevelRead(result2, Pipe.sizeOf(result2, id));
			Pipe.releaseReadLock(result2);
		}

		
	}

        @Ignore
	@Test
	public void testComputeExampleNativeFloat() {
		MatrixTypes type = MatrixTypes.Floats;//Decimals;//Integers; //2, 3328335 longs/ints/doubles   [0,332833152] floats
		
		//TypeMask.Decimal;
				
		int leftRows = 128;
		int leftColumns = 128;

		int rightColumns = 128;				
		int rightRows = leftColumns;		
		
		Random rand = new Random();
		
		float[][] leftTest = new float[leftRows][leftColumns];
		for (int i = 0; i < leftRows; ++i) {
		    for (int j = 0; j < leftColumns; ++j) {
			leftTest[i][j] = rand.nextFloat() * 100;
		    }
		}
		
		float[][] rightTest = new float[rightRows][rightColumns];
		for (int i = 0; i < rightRows; ++i) {
		    for (int j = 0; j < rightColumns; ++j) {
			rightTest[i][j] = rand.nextFloat() * 100;
		    }
		}
								
		MatrixSchema leftSchema = BuildMatrixCompute.buildSchema(leftRows, leftColumns, type);		
		RowSchema<MatrixSchema> leftRowSchema = new RowSchema<MatrixSchema>(leftSchema);
		
		MatrixSchema rightSchema = BuildMatrixCompute.buildSchema(rightRows, rightColumns, type);
		RowSchema<MatrixSchema> rightRowSchema = new RowSchema<MatrixSchema>(rightSchema);
				
		MatrixSchema resultSchema = BuildMatrixCompute.buildResultSchema(leftSchema, rightSchema);
		RowSchema<MatrixSchema> rowResultSchema = new RowSchema<MatrixSchema>(resultSchema);		
		
		
		DecimalSchema result2Schema = new DecimalSchema<MatrixSchema>(resultSchema);

		assertTrue(resultSchema.getRows()==leftRows);
		assertTrue(resultSchema.getColumns()==rightColumns);
		
		assertTrue(leftSchema.getRows()==leftRows);
		assertTrue(leftSchema.getColumns()==leftColumns);
		
		assertTrue(rightSchema.getRows()==rightRows);
		assertTrue(rightSchema.getColumns()==rightColumns);
				
		
		GraphManager gm = new GraphManager();
		
		GraphManager.addDefaultNota(gm, GraphManager.SCHEDULE_RATE, 500);
		
		Pipe<RowSchema<MatrixSchema>> left = new Pipe<RowSchema<MatrixSchema>>(new PipeConfig<RowSchema<MatrixSchema>>(leftRowSchema, leftRows)); 
		Pipe<RowSchema<MatrixSchema>> right = new Pipe<RowSchema<MatrixSchema>>(new PipeConfig<RowSchema<MatrixSchema>>(rightRowSchema, rightRows));
		
		Pipe<RowSchema<MatrixSchema>> result = new Pipe<RowSchema<MatrixSchema>>(new PipeConfig<RowSchema<MatrixSchema>>(rowResultSchema, resultSchema.getRows())); //NOTE: reqires 2 or JSON will not write out !!
		Pipe<DecimalSchema<MatrixSchema>> result2 = new Pipe<DecimalSchema<MatrixSchema>>(new PipeConfig<DecimalSchema<MatrixSchema>>(result2Schema, resultSchema.getRows())); //NOTE: reqires 2 or JSON will not write out !!
		
		
		int targetThreadCount = 12;
		Pipe<ColumnSchema<MatrixSchema>>[] colResults = BuildMatrixCompute.buildGraph(gm, resultSchema, leftSchema, rightSchema, left, right, targetThreadCount-2);
		
		ColumnsToRowsStage<MatrixSchema> ctr = new ColumnsToRowsStage<MatrixSchema>(gm, resultSchema, colResults, result);
		
		
		ConvertToDecimalStage<MatrixSchema> watch = new ConvertToDecimalStage<MatrixSchema>(gm, rowResultSchema, result, result2);
		
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
		
		Pipe.spinBlockForRoom(left, Pipe.EOF_SIZE);
		Pipe.spinBlockForRoom(right, Pipe.EOF_SIZE);
		Pipe.publishEOF(left);
		Pipe.publishEOF(right);
				
		GraphManager.blockUntilStageBeginsShutdown(gm, watch, 500);//timeout in ms
		

		scheduler.awaitTermination(2000, TimeUnit.SECONDS);
		
		float[][] expectedAnswer = new float[leftRows][rightColumns];
		for (int i = 0; i < leftRows; ++i) {
		    for (int j = 0; j < rightRows; ++j) {
			for (int k = 0; k < leftColumns; ++k) {
			    expectedAnswer[i][j] += leftTest[i][k] * rightTest[k][j];
			}
		    }
		}
				
	//	String actual = new String(baos.toByteArray());
		
		for(int r=0;r<leftRows;r++) {
			assertTrue(result2.hasContentToRead(result2));
	
			int id = Pipe.takeMsgIdx(result2);
			for(int c=0;c<rightColumns;c++) {
					
					int exp = Pipe.takeValue(result2);					
					long man = Pipe.takeLong(result2);										
					float value = (float)(man * Math.pow(10, exp));
					assertTrue(Math.abs(expectedAnswer[r][c] - value) < 0.001);
			}
			
			Pipe.confirmLowLevelRead(result2, Pipe.sizeOf(result2, id));
			Pipe.releaseReadLock(result2);
		}

		
	}

    @Ignore("Only for performance test")
	@Test
	public void testComputePerformance() {
	    long startTime, duration, durationMs;
	    startTime = System.nanoTime();
	    testComputeExampleNativeInteger();
	    duration = System.nanoTime() - startTime;
	    durationMs = TimeUnit.NANOSECONDS.toMillis(duration);
	    System.out.println("Native integer run time: " + durationMs + "ms");

	}

}
