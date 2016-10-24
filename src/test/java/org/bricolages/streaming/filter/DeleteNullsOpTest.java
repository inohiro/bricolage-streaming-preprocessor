package org.bricolages.streaming.filter;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class DeleteNullsOpTest extends OpTest {
    @Test
    public void apply() throws Exception {
        val def = new OperatorDefinition(stream("schema.table"), "deletenulls", "*", "{}");
        val op = (DeleteNullsOp)Op.build(def);
        val rec = Record.parse("{\"a\":null,\"b\":1,\"c\":null}");
        val out = op.apply(rec);
        assertEquals("{\"b\":1}", out.serialize());
    }
}
