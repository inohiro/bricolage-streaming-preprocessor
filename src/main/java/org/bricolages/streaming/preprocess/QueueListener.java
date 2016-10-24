package org.bricolages.streaming.preprocess;
import org.bricolages.streaming.event.*;
import org.bricolages.streaming.s3.S3ObjectLocation;
import org.bricolages.streaming.exception.ApplicationAbort;
import org.springframework.beans.factory.annotation.Autowired;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.List;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class QueueListener implements EventHandlers {
    final EventQueue eventQueue;
    final Preprocessor preprocessor;

    static final int INIT_SLEEP_SECONDS = 1;
    static final int MAX_SLEEP_SECONDS = 64;

    public void run() throws IOException {
        log.info("server started");
        trapSignals();
        try {
            boolean emptyPoll = false;
            int sleepLen = INIT_SLEEP_SECONDS;
            while (!isTerminating()) {
                if (emptyPoll) {
                    safeSleep(sleepLen);
                    sleepLen *= 2;
                    if (sleepLen > MAX_SLEEP_SECONDS) {
                        sleepLen = MAX_SLEEP_SECONDS;
                    }
                }
                else {
                    sleepLen = INIT_SLEEP_SECONDS;
                }
                try {
                    emptyPoll = handleEvents();
                    eventQueue.flushDelete();
                }
                catch (SQSException ex) {
                    safeSleep(5);
                }
            }
        }
        catch (ApplicationAbort ex) {
            // ignore
        }
        eventQueue.flushDeleteForce();
        log.info("application is gracefully shut down");
    }

    public void runOnce() throws Exception {
        trapSignals();
        try {
            while (!isTerminating()) {
                val empty = handleEvents();
                if (empty) break;
            }
        }
        catch (ApplicationAbort ex) {
            // ignore
        }
        eventQueue.flushDeleteForce();
    }

    boolean handleEvents() {
        List<Event> events = eventQueue.poll();
        for (Event event : events) {
            log.debug("processing message: {}", event.getMessageBody());
            event.callHandler(this);
        }
        return events.isEmpty();
    }

    @Override
    public void handleUnknownEvent(UnknownEvent event) {
        // FIXME: throttle
        log.error("unknown message: {}", event.getMessageBody());
        eventQueue.deleteAsync(event);
    }

    @Override
    public void handleShutdownEvent(ShutdownEvent event) {
        // Use sync delete to avoid duplicated shutdown
        eventQueue.delete(event);
        initiateShutdown();
    }

    Thread mainThread;
    volatile boolean isTerminating = false;

    void trapSignals() {
        mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                initiateShutdown();
                waitMainThread();
            }
        });
    }

    void initiateShutdown() {
        log.info("initiate shutdown; mainThread={}", mainThread);
        this.isTerminating = true;
        if (mainThread != null) {
            mainThread.interrupt();
        }
    }

    boolean isTerminating() {
        if (isTerminating) return true;
        if (mainThread.isInterrupted()) {
            this.isTerminating = true;
            return true;
        }
        else {
            return false;
        }
    }

    void waitMainThread() {
        if (mainThread == null) return;
        try {
            log.info("waiting main thread...");
            mainThread.join();
        }
        catch (InterruptedException ex) {
            // ignore
        }
    }

    void safeSleep(int sec) {
        try {
            Thread.sleep(sec * 1000);
        }
        catch (InterruptedException ex) {
            this.isTerminating = true;
        }
    }

    @Override
    public void handleS3Event(S3Event event) {
        log.debug("handling URL: {}", event.getLocation().toString());
        preprocessor.processObject(event.getLocation(), !event.doesNotDispatch());
        eventQueue.deleteAsync(event);
    }

    @Override
    public void handleFlushPendingEvent(FlushPendingEvent event) {
        preprocessor.flushDeferedPackets(event.getStreamId(), (packet) -> {
            eventQueue.send(new ProcessPacketEvent(packet.getId(), true));
        });
        eventQueue.delete(event);
    }

    @Override
    public void handleProcessPacketEvent(ProcessPacketEvent event) {
        preprocessor.processPacketById(event.getPacketId(), !event.doesNotDispatch());
        eventQueue.deleteAsync(event);
    }
}
