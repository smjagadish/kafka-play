package com.example.kafkaconfig;

public class processBean {
    public processBean()
    {

    }
    public void intercept()
    {
        System.out.println("interceptor invoked on message send");
    }
    public void custom()
    {
        System.out.println("interceptor invoked on message ack");
    }
}
