package com.fherdelpino.kafka.playground.streams.error;

import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

public class BinanceExchangeUncaughtExceptionHandler implements StreamsUncaughtExceptionHandler {
    @Override
    public StreamThreadExceptionResponse handle(Throwable exception) {
        if (exception instanceof StreamsException) {
            Throwable originalException = exception.getCause();
            if (originalException.getMessage().equals("Retryable transient error")) {
                return StreamThreadExceptionResponse.REPLACE_THREAD;
            }
        }
        return StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
    }
}
