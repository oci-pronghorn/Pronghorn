package com.ociweb.json.encode;

import com.ociweb.json.appendable.StringOut;
import org.junit.Before;
import org.junit.Test;

import java.util.Objects;

import static junit.framework.TestCase.assertEquals;

public class JSONRootArrayTests {
    private StringOut out;

    @Before
    public void init() {
        out = new StringOut();
    }
/*
BUG!!!! index too early
    @Test
    public void testRootArrayEmpty() {
        JSONRenderer<int[]> json = new JSONRenderer<int[]>()
            .array().integer((o, i, v) -> v.visit(0, i < o.length-1));
        json.render(out, new int[0]);
        assertEquals("[]", out.toString());
    }
    */

    @Test
    public void testRootArrayRepeatedNulls() {
        JSONRenderer<int[]> json = new JSONRenderer<int[]>()
                .array((o, i, n)->i<o.length?o:null).constantNull();
        json.render(out, new int[] {9, 8, 7, 6, 5, 4, 3, 2, 1});
        assertEquals("[null,null,null,null,null,null,null,null,null]", out.toString());
    }

    @Test
    public void testRootArrayNull_Yes() {
        JSONRenderer<int[]> json = new JSONRenderer<int[]>()
                .nullableArray(Objects::isNull, (o, i, n)->i<o.length?o:null).integer((o, i, n, v) -> v.visit(o[i]));
        json.render(out, null);
        assertEquals("null", out.toString());
    }

    @Test
    public void testRootArrayNull_No() {
        JSONRenderer<int[]> json = new JSONRenderer<int[]>()
                .nullableArray(Objects::isNull, (o, i, n)->i<o.length?o:null).integer((o, i, n, v) -> v.visit(o[i]));
        json.render(out, new int[] {9, 8, 7, 6, 5, 4, 3, 2, 1});
        assertEquals("[9,8,7,6,5,4,3,2,1]", out.toString());
    }

    @Test
    public void testRootArrayInt() {
        JSONRenderer<int[]> json = new JSONRenderer<int[]>()
                .array((o, i, n)->i<o.length?o:null).integer((o, i, n, v) -> v.visit(o[i]));
        json.render(out, new int[] {9, 8, 7, 6, 5, 4, 3, 2, 1});
        assertEquals("[9,8,7,6,5,4,3,2,1]", out.toString());
    }

    @Test
    public void testRootArrayIntNull() {
        JSONRenderer<int[]> json = new JSONRenderer<int[]>()
                .array((o, i, n)->i<o.length?o:null).integerNull((o, i, n, v) -> v.visit(o[i], (i+2) % 2 == 0));
        json.render(out, new int[] {9, 8, 7, 6, 5, 4, 3, 2, 1});
        assertEquals("[null,8,null,6,null,4,null,2,null]", out.toString());
    }
}
