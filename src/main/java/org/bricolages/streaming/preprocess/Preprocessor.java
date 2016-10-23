package org.bricolages.streaming.preprocess;
import org.bricolages.streaming.stream.*;
import org.bricolages.streaming.filter.*;
import org.bricolages.streaming.s3.*;
import org.bricolages.streaming.exception.ConfigError;
import org.springframework.beans.factory.annotation.Autowired;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.function.Consumer;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class Preprocessor {
    final StreamRouter router;
    final S3Agent s3agent;

    @Autowired
    DataStreamRepository streamRepos;

    public void processObject(S3ObjectLocation location, boolean doesDispatch) {
        DataPacket packet = router.route(location);
        if (packet == null) {
            log.warn("could not detect a stream: {}", location);
            return;
        }
        processPacket(packet, doesDispatch);
    }

    public void processPacketById(long packetId, boolean doesDispatch) {
        processPacket(router.loadPacket(packetId), doesDispatch);
    }

    public void processPacket(DataPacket packet, boolean doesDispatch) {
        DataStream stream = packet.getStream();
        if (stream.isDisabled()) {
            stream.defer(packet);
            return;
        }
        if (stream.doesDiscard()) {
            log.debug("discard event: {}", packet.getLocation());
            return;
        }
        Result result = process(packet);
        if (result != null && doesDispatch) {
            writeDispatchInfo(packet, result);
        }
    }

    public void flushDeferedPackets(long streamId, Consumer<DataPacket> f) {
        DataStream stream = streamRepos.findOne(streamId);
        for (DataPacket packet : stream.deferedPackets()) {
            f.accept(packet);
            stream.clearDefered(packet);
        }
    }

    void writeDispatchInfo(DataPacket packet, Result result) {
        // FIXME
    }

    @NoArgsConstructor
    static final class Result {
        ObjectFilter.Stats stats;
        S3ObjectMetadata metadata;
    }

    @Autowired
    ActivityRepository activityRepos;

    public Result process(DataPacket packet) {
        Activity activity = new Activity(packet);
        activityRepos.save(activity);
        try {
            ObjectFilter filter = packet.getStream().getFilter();
            Result result = applyFilter(filter, packet.getLocation(), packet.getDestination(), packet.getStream().getName());
            log.debug("src: {}, dest: {}, in: {}, out: {}", packet.getLocation(), packet.getDestination(), result.stats.inputRows, result.stats.outputRows);
            activity.succeeded();
            activityRepos.save(activity);
            return result;
        }
        catch (S3IOException | IOException ex) {
            log.error("src: {}, error: {}", packet.getLocation(), ex.getMessage());
            activity.failed(ex.getMessage());
            activityRepos.save(activity);
            return null;
        }
        catch (ConfigError ex) {
            log.error("src: {}, error: {}", packet.getLocation(), ex.getMessage());
            activity.error(ex.getMessage());
            activityRepos.save(activity);
            return null;
        }
    }

    Result applyFilter(ObjectFilter filter, S3ObjectLocation src, S3ObjectLocation dest, String streamName) throws S3IOException, IOException {
        Result result = new Result();
        try (S3Agent.Buffer buf = s3agent.openWriteBuffer(dest, streamName)) {
            try (BufferedReader r = s3agent.openBufferedReader(src)) {
                result.stats = filter.apply(r, buf.getBufferedWriter(), src.toString());
            }
            result.metadata = buf.commit();
            return result;
        }
    }

    public void processOnly(S3ObjectLocation loc, BufferedWriter out) throws S3IOException, IOException {
        DataPacket packet = router.route(loc);
        ObjectFilter filter = packet.getStream().getFilter();
        try (BufferedReader r = s3agent.openBufferedReader(packet.getLocation())) {
            val stats = filter.apply(r, out, packet.getLocation().toString());
            log.debug("src: {}, dest: {}, in: {}, out: {}", packet.getLocation(), packet.getDestination(), stats.inputRows, stats.outputRows);
        }
    }
}
