package com.ociweb.json.encode;

import org.junit.Before;
import org.junit.Test;

import com.ociweb.pronghorn.util.StringBuilderWriter;

import java.util.Arrays;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

public class JSONArrayTests {
    private StringBuilderWriter out;

    @Before
    public void init() {
        out = new StringBuilderWriter();
    }

    @Test
    public void testArray_FromList() {
        JSONRenderer<List<Integer>> json = new JSONRenderer<List<Integer>>()
                .listArray(o->o).integer(List::get);
        assertTrue(json.isLocked());
        json.render(out, Arrays.asList(9, 8, 7, 6, 5, 4, 3, 2, 1));
        assertEquals("[9,8,7,6,5,4,3,2,1]", out.toString());
    }

    @Test
    public void testArray_FromListNull() {
        JSONRenderer<List<Integer>> json = new JSONRenderer<List<Integer>>()
                .listArray(o->o).integer(List::get);
        assertTrue(json.isLocked());
        json.render(out, null);
        assertEquals("null", out.toString());
    }

    @Test
    public void testArray_FromArray() {
        JSONRenderer<Integer[]> json = new JSONRenderer<Integer[]>()
                .basicArray(o->o).integer((o, i) -> o[i]);
        assertTrue(json.isLocked());
        json.render(out, new Integer[] {9, 8, 7, 6, 5, 4, 3, 2, 1});
        assertEquals("[9,8,7,6,5,4,3,2,1]", out.toString());
    }

    @Test
    public void testArray_FromArrayNull() {
        JSONRenderer<Integer[]> json = new JSONRenderer<Integer[]>()
                .basicArray(o->o).integer((o, i) -> o[i]);
        assertTrue(json.isLocked());
        json.render(out, null);
        assertEquals("null", out.toString());
    }

    @Test
    public void testArrayRepeatedNulls() {
        JSONRenderer<int[]> json = new JSONRenderer<int[]>()
                .array((o, i, n)->i<o.length?o:null).constantNull();
        assertTrue(json.isLocked());
        json.render(out, new int[] {9, 8, 7, 6, 5, 4, 3, 2, 1});
        assertEquals("[null,null,null,null,null,null,null,null,null]", out.toString());
    }

    @Test
    public void testArrayNull_Yes() {
        JSONRenderer<int[]> json = new JSONRenderer<int[]>()
                .array(o->o, (o, i, n)->i<o.length?o:null).integer((o, i) -> o[i]);
        assertTrue(json.isLocked());
        json.render(out, null);
        assertEquals("null", out.toString());
    }

    @Test
    public void testArrayNull_No() {
        JSONRenderer<int[]> json = new JSONRenderer<int[]>()
                .array(o->o, (o, i, n)->i<o.length?o:null).integer((o, i) -> o[i]);
        assertTrue(json.isLocked());
        json.render(out, new int[] {9, 8, 7, 6, 5, 4, 3, 2, 1});
        assertEquals("[9,8,7,6,5,4,3,2,1]", out.toString());
    }

    @Test
    public void testArrayBool() {
        JSONRenderer<boolean[]> json = new JSONRenderer<boolean[]>()
                .array((o, i, n)->i<o.length?o:null).bool((o, i) -> o[i]);
        assertTrue(json.isLocked());
        json.render(out, new boolean[] {true, true, false, false, true, false, true, false});
        assertEquals("[true,true,false,false,true,false,true,false]", out.toString());
    }

    @Test
    public void testArrayBool_Null() {
        JSONRenderer<boolean[]> json = new JSONRenderer<boolean[]>()
                .array((o, i, n)->i<o.length?o:null).nullableBool((o, i) -> i==2, (o, i) -> o[i]);
        assertTrue(json.isLocked());
        json.render(out, new boolean[] {true, true, false, false, true, false, true, false});
        assertEquals("[true,true,null,false,true,false,true,false]", out.toString());
    }

    @Test
    public void testArrayInt() {
        JSONRenderer<int[]> json = new JSONRenderer<int[]>()
                .array((o, i, n)->i<o.length?o:null).integer((o, i) -> o[i]);
        assertTrue(json.isLocked());
        json.render(out, new int[] {9, 8, 7, 6, 5, 4, 3, 2, 1});
        assertEquals("[9,8,7,6,5,4,3,2,1]", out.toString());
    }

    @Test
    public void testArrayInt_Null() {
        JSONRenderer<int[]> json = new JSONRenderer<int[]>()
                .array((o, i, n)->i<o.length?o:null).nullableInteger((o, i) -> i==2, (o, i) -> o[i]);
        assertTrue(json.isLocked());
        json.render(out, new int[] {9, 8, 7, 6, 5, 4, 3, 2, 1});
        assertEquals("[9,8,null,6,5,4,3,2,1]", out.toString());
    }

    @Test
    public void testArrayDouble() {
        JSONRenderer<double[]> json = new JSONRenderer<double[]>()
                .array((o, i, n)->i<o.length?o:null).decimal(2, (o, i) -> o[i]);
        assertTrue(json.isLocked());
        json.render(out, new double[] {9.765, 0.8, 7.0009, 6.1, 0.00004});
        assertEquals("[9.76,0.80,7.00,6.10,0.00]", out.toString());
    }

    @Test
    public void testArrayDouble_Null() {
        JSONRenderer<double[]> json = new JSONRenderer<double[]>()
                .array((o, i, n)->i<o.length?o:null).nullableDecimal(2, (o, i) -> i==2, (o, i) -> o[i]);
        assertTrue(json.isLocked());
        json.render(out, new double[] {9.765, 0.8, 7.0009, 6.1, 0.00004});
        assertEquals("[9.76,0.80,null,6.10,0.00]", out.toString());
    }

    @Test
    public void testArrayString() {
        JSONRenderer<String[]> json = new JSONRenderer<String[]>()
                .basicArray(o->o).string((o, i) -> o[i]);
        assertTrue(json.isLocked());
        json.render(out, new String[] {"hello", "there"});
        assertEquals("[\"hello\",\"there\"]", out.toString());
    }

    @Test
    public void testArrayString_Null() {
        JSONRenderer<String[]> json = new JSONRenderer<String[]>()
                .basicArray(o->o).nullableString((o, i) -> o[i]);
        assertTrue(json.isLocked());
        json.render(out, new String[] {"hello", null});
        assertEquals("[\"hello\",null]", out.toString());
    }

    @Test
    public void testArrayEnum_name() {
        JSONRenderer<StackEnum[]> json = new JSONRenderer<StackEnum[]>()
                .basicArray(o->o).enumName((o, i) -> o[i]);
        assertTrue(json.isLocked());
        json.render(out, new StackEnum[]{null, StackEnum.pronghornPipes, null, StackEnum.greenlightning});
        assertEquals("[null,\"pronghornPipes\",null,\"greenlightning\"]", out.toString());
    }

    @Test
    public void testArrayEnum_ordinal() {
        JSONRenderer<StackEnum[]> json = new JSONRenderer<StackEnum[]>()
                .basicArray(o->o).enumOrdinal((o, i) -> o[i]);
        assertTrue(json.isLocked());
        json.render(out, new StackEnum[]{null, StackEnum.pronghornPipes, null, StackEnum.greenlightning});
        assertEquals("[null,1,null,3]", out.toString());
    }
}
