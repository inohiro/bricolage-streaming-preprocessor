package org.bricolages.streaming.filter;
import java.util.List;
import java.util.stream.Collectors;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.PrintWriter;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ObjectFilter {
    static public ObjectFilter build(List<OperatorDefinition> defs) {
        defs.sort((x, y) -> x.applicationOrder - y.applicationOrder);
        List<Op> ops = defs.stream().map((def) -> {
            Op op = Op.build(def);
            log.debug("operator stacked: {}", op);
            return op;
        }).collect(Collectors.toList());
        return new ObjectFilter(ops);
    }

    final List<Op> operators;

    public ObjectFilter(List<Op> operators) {
        this.operators = operators;
    }

    public Stats apply(BufferedReader r, BufferedWriter w, String sourceName) throws IOException {
        final Stats stats = new Stats();
        final PrintWriter out = new PrintWriter(w);
        r.lines().forEach((line) -> {
            if (line.trim().isEmpty()) return;  // should not count blank line
            stats.inputRows++;
            try {
                String outStr = applyString(line);
                if (outStr != null) {
                    out.println(outStr);
                    stats.outputRows++;
                }
            }
            catch (JSONException ex) {
                log.debug("JSON parse error: {}:{}: {}", sourceName, stats.inputRows, ex.getMessage());
                stats.errorRows++;
            }
        });
        return stats;
    }

    static public final class Stats {
        public int inputRows;
        public int outputRows;
        public int errorRows;
    }

    public String applyString(String json) throws JSONException {
        Record record = Record.parse(json);
        if (record == null) return null;
        for (Op op : operators) {
            record = op.apply(record);
            if (record == null) return null;
        }
        return record.serialize();
    }
}
