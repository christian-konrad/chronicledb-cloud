package de.umr.raft.raftlogreplicationdemo.controllers;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class ExceptionHandlerAdvice {

    @ExceptionHandler(ClassNotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public String handleNotFoundException(ClassNotFoundException ex) {
        //ResponseMsg responseMsg = new ResponseMsg(ex.getMessage());
        return "Class not found: " + ex.getMessage();
    }

}
