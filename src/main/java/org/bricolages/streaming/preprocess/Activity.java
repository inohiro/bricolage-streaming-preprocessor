package org.bricolages.streaming.preprocess;
import org.bricolages.streaming.stream.DataPacket;
import javax.persistence.*;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@Entity
@Table(name="strload_preproc_jobs")
@Slf4j
public class Activity {
    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    @Column(name="preproc_job_id", nullable=false)
    long id;

    @ManyToOne(optional=false, fetch=FetchType.LAZY)
    @JoinColumn(name="packat_id", nullable=false)
    DataPacket packet;

    static final String STATUS_RUNNING = "running";
    static final String STATUS_SUCCESS = "success";
    static final String STATUS_FAILURE = "failure";
    static final String STATUS_ERROR = "error";

    @Column(name="status")
    String status = STATUS_RUNNING;

    @Column(name="start_time", nullable=false)
    Timestamp startTime = Timestamp.from(Instant.now());

    @Column(name="finish_time", nullable=false)
    Timestamp finishTime;

    @Column(name="message", nullable=false)
    String message = "started";

    public Activity(DataPacket packet) {
        this.packet = packet;
    }

    public void succeeded() {
        this.status = STATUS_SUCCESS;
        this.message = "";
    }

    public void failed(String msg) {
        this.status = STATUS_FAILURE;
        this.message = msg;
    }

    public void error(String msg) {
        this.status = STATUS_ERROR;
        this.message = msg;
    }
}
