package com.pralay.HbaseFullLoad;

/// Used for signaling errors, which can diminish in a while, so it worth retrying
public class RetryableException extends RuntimeException {
    public RetryableException(Exception ex) {
        super(ex);
    }
}
