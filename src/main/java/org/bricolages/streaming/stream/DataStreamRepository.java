package org.bricolages.streaming.stream;
import org.bricolages.streaming.exception.ApplicationError;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;

public interface DataStreamRepository extends JpaRepository<DataStream, Long> {
    List<DataStream> findByName(String streamName);

    default DataStream findStream(String streamName) {
        List<DataStream> list = findByName(streamName);
        if (list.isEmpty()) return null;
        if (list.size() > 1) {
            throw new ApplicationError("FATAL: multiple stream matched: " + streamName);
        }
        return list.get(0);
    }
}
