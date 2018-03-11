package com.ociweb.json.encode;

import com.ociweb.json.appendable.StringBuilderWriter;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

public class JSONRootArrayTests {
    private StringBuilderWriter out;

    @Before
    public void init() {
        out = new StringBuilderWriter();
    }

    @Test
    public void testRootListArray() {
        JSONRenderer<List<Integer>> json = new JSONRenderer<List<Integer>>()
                .listArray(o->o).integer((o, i, n, v) -> v.visit(n));
        assertTrue(json.isLocked());
        json.render(out, Arrays.asList(9, 8, 7, 6, 5, 4, 3, 2, 1));
        assertEquals("[9,8,7,6,5,4,3,2,1]", out.toString());
    }

    @Test
    public void testRootListArray_null() {
        JSONRenderer<List<Integer>> json = new JSONRenderer<List<Integer>>()
                .listArray(o->o).integer((o, i, n, v) -> v.visit(n));
        assertTrue(json.isLocked());
        json.render(out, null);
        assertEquals("null", out.toString());
    }

    @Test
    public void testRootArrayArray() {
        JSONRenderer<Integer[]> json = new JSONRenderer<Integer[]>()
                .basicArray(o->o).integer((o, i, n, v) -> v.visit(n));
        assertTrue(json.isLocked());
        json.render(out, new Integer[] {9, 8, 7, 6, 5, 4, 3, 2, 1});
        assertEquals("[9,8,7,6,5,4,3,2,1]", out.toString());
    }

    @Test
    public void testRootArrayArray_null() {
        JSONRenderer<Integer[]> json = new JSONRenderer<Integer[]>()
                .basicArray(o->o).integer((o, i, n, v) -> v.visit(n));
        assertTrue(json.isLocked());
        json.render(out, null);
        assertEquals("null", out.toString());
    }

    @Test
    public void testRootArrayRenderer() {
        JSONRenderer<Double> json1 = new JSONRenderer<Double>()
                .decimal(3, o->o);
        JSONRenderer<Integer[]> json = new JSONRenderer<Integer[]>()
                .basicArray(o->o).renderer(json1, (o, i, node) -> i != 3 ? o[i].doubleValue() + i : null);
        assertTrue(json.isLocked());
        json.render(out, new Integer[] {9, 9, 9, 9, 9, 9, 9, 9, 9});
        assertEquals("[9.000,10.000,11.000,null,13.000,14.000,15.000,16.000,17.000]", out.toString());
    }

    @Test
    public void testRootArrayRepeatedNulls() {
        JSONRenderer<int[]> json = new JSONRenderer<int[]>()
                .array((o, i, n)->i<o.length?o:null).constantNull();
        assertTrue(json.isLocked());
        json.render(out, new int[] {9, 8, 7, 6, 5, 4, 3, 2, 1});
        assertEquals("[null,null,null,null,null,null,null,null,null]", out.toString());
    }

    @Test
    public void testRootArrayNull_Yes() {
        JSONRenderer<int[]> json = new JSONRenderer<int[]>()
                .array(o->o, (o, i, n)->i<o.length?o:null).integer((o, i, n, v) -> v.visit(o[i]));
        assertTrue(json.isLocked());
        json.render(out, null);
        assertEquals("null", out.toString());
    }

    @Test
    public void testRootArrayNull_No() {
        JSONRenderer<int[]> json = new JSONRenderer<int[]>()
                .array(o->o, (o, i, n)->i<o.length?o:null).integer((o, i, n, v) -> v.visit(o[i]));
        assertTrue(json.isLocked());
        json.render(out, new int[] {9, 8, 7, 6, 5, 4, 3, 2, 1});
        assertEquals("[9,8,7,6,5,4,3,2,1]", out.toString());
    }

    @Test
    public void testRootArrayObject() {
        JSONRenderer<BasicObj[]> json = new JSONRenderer<BasicObj[]>()
                .array((o, i, n)->i<o.length?o:null)
                .beginObject((obj, i, node) -> obj[i])
                .bool("b", o->o.b)
                .integer("i", o->o.i)
                .decimal("d", 2, o->o.d)
                .string("s", o->o.s)
                .beginObject("m")
                .endObject()
                .endObject();
        assertTrue(json.isLocked());
        json.render(out, new BasicObj[] {new BasicObj(43), null, new BasicObj(44)});
        assertEquals("[{\"b\":true,\"i\":43,\"d\":123.40,\"s\":\"fum\",\"m\":{}},null,{\"b\":true,\"i\":44,\"d\":123.40,\"s\":\"fum\",\"m\":{}}]", out.toString());
    }

    @Test
    public void testRootArrayIntNull() {
        JSONRenderer<int[]> json = new JSONRenderer<int[]>()
                .array((o, i, n)->i<o.length?o:null).integerNull((o, i, n, v) -> v.visit(o[i], (i+2) % 2 == 0));
        assertTrue(json.isLocked());
        json.render(out, new int[] {9, 8, 7, 6, 5, 4, 3, 2, 1});
        assertEquals("[null,8,null,6,null,4,null,2,null]", out.toString());
    }

    @Test
    public void testRootArrayBool() {
        JSONRenderer<boolean[]> json = new JSONRenderer<boolean[]>()
                .array((o, i, n)->i<o.length?o:null).bool((o, i, n, v) -> v.visit(o[i]));
        assertTrue(json.isLocked());
        json.render(out, new boolean[] {true, true, false, false, true, false, true, false});
        assertEquals("[true,true,false,false,true,false,true,false]", out.toString());
    }

    @Test
    public void testRootArrayInt() {
        JSONRenderer<int[]> json = new JSONRenderer<int[]>()
                .array((o, i, n)->i<o.length?o:null).integer((o, i, n, v) -> v.visit(o[i]));
        assertTrue(json.isLocked());
        json.render(out, new int[] {9, 8, 7, 6, 5, 4, 3, 2, 1});
        assertEquals("[9,8,7,6,5,4,3,2,1]", out.toString());
    }

    @Test
    public void testRootArrayDouble() {
        JSONRenderer<double[]> json = new JSONRenderer<double[]>()
                .array((o, i, n)->i<o.length?o:null).decimal(2, (o, i, n, v) -> v.visit(o[i]));
        assertTrue(json.isLocked());
        json.render(out, new double[] {9.765, 0.8, 7.0009, 6.1, 0.00004});
        assertEquals("[9.76,0.80,7.00,6.10,0.00]", out.toString());
    }

    @Test
    public void testRootArrayString() {
        JSONRenderer<String[]> json = new JSONRenderer<String[]>()
                .basicArray(o->o).string((o, i, n, v) -> v.visit(o[i]));
        assertTrue(json.isLocked());
        json.render(out, new String[] {"hello", "there"});
        assertEquals("[\"hello\",\"there\"]", out.toString());
    }
}
