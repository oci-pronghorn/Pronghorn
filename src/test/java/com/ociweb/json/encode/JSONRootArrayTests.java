package com.ociweb.json.encode;

import com.ociweb.json.appendable.StringBuilderWriter;
import org.junit.Before;
import org.junit.Test;

import java.util.Objects;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

public class JSONRootArrayTests {
    private StringBuilderWriter out;

    @Before
    public void init() {
        out = new StringBuilderWriter();
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
                .nullableArray(Objects::isNull, (o, i, n)->i<o.length?o:null).integer((o, i, n, v) -> v.visit(o[i]));
        assertTrue(json.isLocked());
        json.render(out, null);
        assertEquals("null", out.toString());
    }

    @Test
    public void testRootArrayNull_No() {
        JSONRenderer<int[]> json = new JSONRenderer<int[]>()
                .nullableArray(Objects::isNull, (o, i, n)->i<o.length?o:null).integer((o, i, n, v) -> v.visit(o[i]));
        assertTrue(json.isLocked());
        json.render(out, new int[] {9, 8, 7, 6, 5, 4, 3, 2, 1});
        assertEquals("[9,8,7,6,5,4,3,2,1]", out.toString());
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
    public void testRootArrayIntNull() {
        JSONRenderer<int[]> json = new JSONRenderer<int[]>()
                .array((o, i, n)->i<o.length?o:null).integerNull((o, i, n, v) -> v.visit(o[i], (i+2) % 2 == 0));
        assertTrue(json.isLocked());
        json.render(out, new int[] {9, 8, 7, 6, 5, 4, 3, 2, 1});
        assertEquals("[null,8,null,6,null,4,null,2,null]", out.toString());
    }

    @Test
    public void testRootArrayObject() {
        JSONRenderer<BasicObj[]> json = new JSONRenderer<BasicObj[]>()
                .array((o, i, n)->i<o.length?o:null)
                    .beginObject((obj, i, node) -> obj[i])
                        .bool("b", o->o.b)
                        .integer("i", o->o.i)
                        .decimal("d", (o, v) -> v.visit(o.d, 2))
                        .string("s", o->o.s)
                        .beginObject("m")
                        .endObject()
                    .endObject();
        assertTrue(json.isLocked());
        json.render(out, new BasicObj[] {new BasicObj(43), new BasicObj(44)});
        assertEquals("[{\"b\":true,\"i\":43,\"d\":123.40,\"s\":\"fum\",\"m\":{}},{\"b\":true,\"i\":44,\"d\":123.40,\"s\":\"fum\",\"m\":{}}]", out.toString());
    }
}
