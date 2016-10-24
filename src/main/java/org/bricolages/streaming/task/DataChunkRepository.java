package org.bricolages.streaming.task;
import org.springframework.data.jpa.repository.JpaRepository;

public interface DataChunkRepository extends JpaRepository<DataChunk, Long> {
}
