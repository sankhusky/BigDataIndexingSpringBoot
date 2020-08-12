package com.sanket.demo7255.rabbitmq;

import java.util.concurrent.TimeUnit;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import com.sanket.demo7255.Demo7255Application;

//@Component
public class Runner  {

  private final RabbitTemplate rabbitTemplate;
  private final IndexingListener receiver;

  public Runner(IndexingListener receiver, RabbitTemplate rabbitTemplate) {
    this.receiver = receiver;
    this.rabbitTemplate = rabbitTemplate;
  }

//  @Override
//  public void run(String... args) throws Exception {
//    System.out.println("Sending message...");
//    rabbitTemplate.convertAndSend(Demo7255Application.topicExchange, "sanket.indexing.baz", args[0]);
//    
//    receiver.getLatch().await(10000, TimeUnit.MILLISECONDS);
//  }

}
