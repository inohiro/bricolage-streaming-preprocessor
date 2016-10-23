package org.bricolages.streaming.stream;
import org.bricolages.streaming.filter.ObjectFilter;
import org.bricolages.streaming.filter.OperatorDefinition;
import org.bricolages.streaming.filter.OperatorDefinitionRepository;
import org.springframework.beans.factory.annotation.Autowired;
import javax.persistence.*;
import java.sql.Timestamp;
import java.util.List;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@NoArgsConstructor
@AllArgsConstructor
@ToString
@Entity
@Table(name="strload_streams")
@Slf4j
public class DataStream {
    @Id
    @Column(name="stream_id", nullable=false)
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    long id;

    @Getter
    @Column(name="stream_name", nullable=false)
    String name;

    @Column(name="stream_prefix", nullable=false)
    String prefix;

    // We do not use table entity, just treat as an integer
    @Column(name="table_id")
    Long tableId = null;

    @OneToMany(fetch=FetchType.EAGER, mappedBy="stream")
    List<OperatorDefinition> operatorDefinitions;

    public ObjectFilter getFilter() {
        return ObjectFilter.build(operatorDefinitions);
    }

    @Column(name="disabled", nullable=false)
    boolean disabled = false;

    @Column(name="discard", nullable=false)
    boolean discard = false;

    public DataStream(String name, String prefix) {
        this.name = name;
        this.prefix = prefix;
        this.disabled = true;
    }

    public boolean isDisabled() {
        return this.disabled;
    }

    public boolean doesDiscard() {
        return this.discard;
    }

    @Autowired
    DataPacketRepository packetRepos;

    public void defer(DataPacket packet) {
        packet.setPending(true);
        packetRepos.save(packet);
    }

    public void clearDefered(DataPacket packet) {
        packet.setPending(false);
        packetRepos.save(packet);
    }

    public List<DataPacket> deferedPackets() {
        return packetRepos.findByStreamId(id);
    }
}
