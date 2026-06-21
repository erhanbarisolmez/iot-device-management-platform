package com.selftech.smartlock.utils.exceptions;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
public class SmsSendingException extends RuntimeException {

    public SmsSendingException(String message, Throwable cause) {
        super(message, cause);
    }
}