package org.bricolages.streaming.stream;
import org.bricolages.streaming.s3.S3ObjectLocation;
import org.hibernate.annotations.Proxy;
import javax.persistence.*;
import java.sql.Timestamp;
import java.util.List;
import lombok.*;

@NoArgsConstructor
@AllArgsConstructor
@ToString
@Entity
@Proxy(lazy=false)
@Table(name="strload_packets")
public class DataPacket {
    @Getter
    @Id
    @Column(name="packet_id", nullable=false)
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    long id;

    @Getter
    @ManyToOne(optional=false, fetch=FetchType.EAGER)
    @JoinColumn(name="stream_id", nullable=false)
    DataStream stream;

    @Column(name="object_url", nullable=false)
    String objectUrl;

    @Getter
    @Transient
    S3ObjectLocation location;

    @Getter
    @Column(name="create_time", nullable=false)
    public Timestamp createTime;

    @Setter
    @Column(name="pending", nullable=false)
    boolean pending = false;

    @Getter
    @Transient
    S3ObjectLocation destination;

    public DataPacket(DataStream stream, S3ObjectLocation location, S3ObjectLocation destination) {
        this.stream = stream;
        this.location = location;
        this.objectUrl = location.urlString();
        this.destination = destination;
    }
}
