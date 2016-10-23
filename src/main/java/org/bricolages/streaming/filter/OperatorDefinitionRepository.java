package org.bricolages.streaming.filter;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;

public interface OperatorDefinitionRepository extends JpaRepository<OperatorDefinition, Long> {
    List<OperatorDefinition> findByStreamIdOrderByApplicationOrderAsc(long streamId);
}
