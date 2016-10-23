package org.bricolages.streaming.preprocess;
import org.bricolages.streaming.stream.DataStream;
import org.bricolages.streaming.exception.ApplicationError;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;

public interface ActivityRepository extends JpaRepository<Activity, Long> {
}
