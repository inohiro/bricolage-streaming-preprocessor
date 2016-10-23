package org.bricolages.streaming.stream;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;

public interface DataPacketRepository extends JpaRepository<DataPacket, Long> {
    List<DataPacket> findByStreamId(long streamId);
}
