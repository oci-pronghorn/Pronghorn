package com.ociweb.json.encode;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.ociweb.pronghorn.util.StringBuilderWriter;

public class JSONArrayCompoundTests {
    private StringBuilderWriter out;

    @Before
    public void init() {
        out = new StringBuilderWriter();
    }

    static class Owner {
        List<Integer> l = Arrays.asList(9, 8, 7, 6, 5, 4, 3, 2, 1);
    }

    @Test
    public void testArray_FromOwnedList() {
        JSONRenderer<Owner> json = new JSONRenderer<Owner>()
                .listArray(o->o.l).integer(List::get);
        assertTrue(json.isLocked());
        json.render(out, new Owner());
        assertEquals("[9,8,7,6,5,4,3,2,1]", out.toString());
    }

    @Test
    public void testArrayRenderer() {
        JSONRenderer<Double> json1 = new JSONRenderer<Double>()
                .decimal(3, o->o);
        JSONRenderer<Integer[]> json = new JSONRenderer<Integer[]>()
                .basicArray(o->o).renderer(json1, (o, i) -> i != 3 ? o[i].doubleValue() + i : null);
        assertTrue(json.isLocked());
        json.render(out, new Integer[] {9, 9, 9, 9, 9, 9, 9, 9, 9});
        assertEquals("[9.000,10.000,11.000,null,13.000,14.000,15.000,16.000,17.000]", out.toString());
    }

    @Test
    public void testArrayObject() {
        JSONRenderer<BasicObj[]> json = new JSONRenderer<BasicObj[]>()
                .array((o, i, n) -> i<o.length?o:null)
                .startObject((o, i) -> o[i])
                    .bool("b", o->o.b)
                    .integer("i", o->o.i)
                    .decimal("d", 2, o->o.d)
                    .string("s", (o,t)-> t.append(o.s))
                    .startObject("m")
                    .endObject()
                .endObject();
        assertTrue(json.isLocked());
        json.render(out, new BasicObj[] {new BasicObj(43), null, new BasicObj(44)});
        assertEquals("[{\"b\":true,\"i\":43,\"d\":123.40,\"s\":\"fum\",\"m\":{}},null,{\"b\":true,\"i\":44,\"d\":123.40,\"s\":\"fum\",\"m\":{}}]", out.toString());
    }

    @Test
    public void testArrayArray_FromArray_empty() {
        JSONRenderer<int[][][]> json = new JSONRenderer<int[][][]>()
                // for i in root
                .array(o->o, (o, i, node) -> (i < o.length ? o[i] : null))
                    // for j in o[i]
                    .array((o, i) -> o[i], (o, j, node) -> (j < o.length ? o : null))
                        // for k in o[i][j]
                        .array((o, j) -> o[j], (o, k, node) -> (k < o.length ? o : null))
                            // v = o[i][j][k]
                            .integer((o, k) -> o[k]);
        assertTrue(json.isLocked());
        json.render(out, new int[][][] {{{}}});
        assertEquals("[[[]]]", out.toString());
    }

    @Test
    public void testArrayArray_FromArray_NotEmpty() {
        JSONRenderer<int[][][]> json = new JSONRenderer<int[][][]>()
                .array(o->o, (o, i, node) -> (i < o.length ? o[i] : null))
                .array((o, i) -> o[i], (o, j, node) -> (j < o.length ? o : null))
                .array((o, j) -> o[j], (o, k, node) -> (k < o.length ? o : null))
                .integer((o, k) -> o[k]);
        assertTrue(json.isLocked());
        json.render(out, new int[][][]
                {{{1, 2, 3},{1, 2},{1},{}},{{1, 2},{1},{}},{{1},{}},{{}},{}});
        assertEquals("[[[1,2,3],[1,2],[1],[]],[[1,2],[1],[]],[[1],[]],[[]],[]]", out.toString());
    }

    @Test
    public void testArrayArray_FromArray_Null() {
        JSONRenderer<int[][]> json = new JSONRenderer<int[][]>()
                .array(o->o, (o, i, node) -> (i < o.length ? o : null))
                .array((o, i) -> o[i], (o, j, node) -> (j < o.length ? o : null))
                .integer((o, j) -> o[j]);
        assertTrue(json.isLocked());
        json.render(out, new int[][] {{1, 2, 3}, null, {4, 5, 6}});
        assertEquals("[[1,2,3],null,[4,5,6]]", out.toString());
    }

    @Test
    public void testArrayArray_FromList1_Null() {
        JSONRenderer<List<List<Integer>>> json = new JSONRenderer<List<List<Integer>>>()
                .listArray(o->o)
                .listArray(List::get)
                .integer(List::get);
        assertTrue(json.isLocked());
        json.render(out, Arrays.asList(Arrays.asList(1, 2, 3), null, Arrays.asList(4, 5, 6)));
        assertEquals("[[1,2,3],null,[4,5,6]]", out.toString());
    }

    @Test
    public void testArrayArray_FromList2_Null() {
        JSONRenderer<List<List<Integer>>> json = new JSONRenderer<List<List<Integer>>>()
                .listArray(o->o)
                .iterArray(List::get)
                .integer((o, i) -> o.next());
        assertTrue(json.isLocked());
        json.render(out, Arrays.asList(Arrays.asList(1, 2, 3), null, Arrays.asList(4, 5, 6)));
        assertEquals("[[1,2,3],null,[4,5,6]]", out.toString());
    }

    @Test
    public void testArrayArray_FromBasic_Null() {
        JSONRenderer<Integer[][]> json = new JSONRenderer<Integer[][]>()
                .basicArray(o->o)
                .basicArray((o, i) -> o[i])
                .integer((o, j) -> o[j]);
        assertTrue(json.isLocked());
        json.render(out, new Integer[][] {{1, 2, 3}, null, {4, 5, 6}});
        assertEquals("[[1,2,3],null,[4,5,6]]", out.toString());
    }
}
