package com.app.emailnotification.error;

public class NonRetryableException extends RuntimeException {

    public NonRetryableException(String message) {
        super(message);
    }


    public NonRetryableException(Throwable cause) {
        super(cause);
    }

}
