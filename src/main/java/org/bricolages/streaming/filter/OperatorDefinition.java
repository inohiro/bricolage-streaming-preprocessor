package org.bricolages.streaming.filter;
import org.bricolages.streaming.stream.DataStream;
import org.bricolages.streaming.exception.ConfigError;
import javax.persistence.*;
import java.util.List;
import java.io.IOException;
import java.sql.Timestamp;
import lombok.*;

@NoArgsConstructor
@AllArgsConstructor
@ToString
@Entity
@Table(name="strload_filters")
public class OperatorDefinition {
    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    @Column(name="filter_id")
    long filter_id;

    @Getter
    @ManyToOne(optional=false, fetch=FetchType.LAZY)
    @JoinColumn(name="stream_id", nullable=false)
    DataStream stream;

    @Column(name="operator_id")
    @Getter
    String operatorId;

    @Column(name="target_column")
    String targetColumn;

    @Getter
    @Column(name="application_order")
    int applicationOrder;

    @Column(name="params")
    String params;  // JSON string

    @Column(name="created_at")
    Timestamp createdTime;

    @Column(name="updated_at")
    Timestamp updatedTime;

    // For tests
    OperatorDefinition(DataStream stream, String operatorId, String targetColumn, String params) {
        this(0, stream, operatorId, targetColumn, 0, params, null, null);
    }

    public boolean isSingleColumn() {
        return ! this.targetColumn.equals("*");
    }

    public String getTargetColumn() {
        if (!isSingleColumn()) throw new ConfigError("is not a single column op: " + stream.getName() + ", " + operatorId);
        return targetColumn;
    }

    public <T> T mapParameters(Class<T> type) {
        try {
            val map = new com.fasterxml.jackson.databind.ObjectMapper();
            return map.readValue(params, type);
        }
        catch (IOException err) {
            throw new ConfigError("could not map filter parameters: " + stream.getName() + "." + targetColumn + "[" + operatorId + "]: " + params + ": " + err.getMessage());
        }
    }
}
