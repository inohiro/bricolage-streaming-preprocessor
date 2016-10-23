package org.bricolages.streaming.event;

public interface EventHandlers {
    void handleS3Event(S3Event e);
    void handleFlushPendingEvent(FlushPendingEvent e);
    void handleProcessPacketEvent(ProcessPacketEvent e);
    void handleShutdownEvent(ShutdownEvent e);
    void handleUnknownEvent(UnknownEvent e);
}
