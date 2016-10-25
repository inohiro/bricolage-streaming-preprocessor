package org.bricolages.streaming.task;
import org.bricolages.streaming.stream.DataStream;
import org.bricolages.streaming.stream.DataPacket;
import javax.persistence.*;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import lombok.*;

@NoArgsConstructor
@AllArgsConstructor
@ToString
@Entity
@Table(name="strload_objects")
public class DataChunk {
    @Getter
    @Id
    @Column(name="object_id", nullable=false)
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    Long id = null;

    @Column(name="object_url", nullable=false)
    String objectUrl;

    @OneToOne(optional=false, fetch=FetchType.LAZY)
    @JoinColumn(name="packet_id", nullable=false)
    DataPacket packet;

    @ManyToOne(optional=false, fetch=FetchType.LAZY)
    @JoinColumn(name="stream_id", nullable=false)
    DataStream stream;

    @Column(name="submit_time", nullable=false)
    public Timestamp submitTime = Timestamp.from(Instant.now());

    @Column(name="object_size", nullable=false)
    int objectSize;

    @Column(name="row_count", nullable=false)
    int rowCount;

    @Column(name="error_row_count", nullable=false)
    int errorRowCount;

    @Column(name="loaded", nullable=false)
    boolean loaded = false;

    public DataChunk(DataPacket packet, int objectSize, int rowCount, int errorRowCount) {
        this.packet = packet;
        this.objectUrl = packet.getDestination().toString();
        this.stream = packet.getStream();
        this.objectSize = objectSize;
        this.rowCount = rowCount;
        this.errorRowCount = errorRowCount;
    }
}
