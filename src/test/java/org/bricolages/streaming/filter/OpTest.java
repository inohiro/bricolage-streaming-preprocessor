package org.bricolages.streaming.filter;
import org.bricolages.streaming.stream.DataStream;

abstract class OpTest {
    DataStream stream(String name) {
        return new DataStream(name, name);
    }
}
