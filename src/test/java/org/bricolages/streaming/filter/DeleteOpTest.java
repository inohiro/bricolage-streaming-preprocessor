package org.bricolages.streaming.filter;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class DeleteOpTest extends OpTest {
    @Test
    public void apply() throws Exception {
        val def = new OperatorDefinition(stream("schema.table"), "delete", "b", "{}");
        val op = (DeleteOp)Op.build(def);
        val rec = Record.parse("{\"a\":1,\"b\":2,\"c\":3}");
        val out = op.apply(rec);
        assertEquals("{\"a\":1,\"c\":3}", out.serialize());
    }
}
