package com.example.sqs_poc;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.sqs.AbstractAmazonSQS;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.aws.autoconfigure.context.ContextStackAutoConfiguration;
import org.springframework.cloud.aws.messaging.core.QueueMessagingTemplate;
import org.springframework.cloud.aws.messaging.listener.Acknowledgment;
import org.springframework.cloud.aws.messaging.listener.SqsMessageDeletionPolicy;
import org.springframework.cloud.aws.messaging.listener.annotation.SqsListener;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import javax.websocket.Session;
import java.util.List;
import java.util.Map;


@SpringBootApplication(exclude = {ContextStackAutoConfiguration.class})
@RestController
public class SqsPocApplication {


    Logger logger = LoggerFactory.getLogger(SqsPocApplication.class);




//    @Autowired
//    private QueueMessagingTemplate queueMessagingTemplate;

    @Autowired
    private AmazonSQS sqsInst;




    @Value("${cloud.aws.end-point.uri}")
    private String endpoint;

    private final int msgCount = 1000;

    private final String grp1 = "m1";
    private final String grp2 = "m2";

    // Each message gets sent "msgCount" times and message group id changes alternately between messageGroupID1 and messageGroupID2
    @GetMapping("/send/{message}")
    public void sendMessageToQueue(@PathVariable String message){
            for(int i=0; i<=msgCount; i++){

                String currMessage = message + String.valueOf(i);

                String currMsgGrpId = "";
                if(i%2 == 0)
                    currMsgGrpId = grp1;
                else
                    currMsgGrpId = grp2;

//                    queueMessagingTemplate.send(endpoint, MessageBuilder.withPayload(currMessage)
//                            .setHeader("message-group-id", currMsgGrpId)
//                            .build());
                    SendMessageRequest sendMessageRequest = new SendMessageRequest()
                            .withQueueUrl(endpoint).withMessageBody(currMessage).withMessageGroupId(currMsgGrpId);

                    sqsInst.sendMessage(sendMessageRequest);

             //   logger.info("message sent to queue: {}", currMessage);
            }


    }


//    ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
//            .queueUrl(endpoint)
//            .waitTimeSeconds(20) // force long polling with 20 seconds
//            .attributeNamesWithStrings("All") // return all attributes
//            .build();
//    List<Message> messages = this.sqsClient.receiveMessage(receiveMessageRequest).messages();



    Logger logger1 = LoggerFactory.getLogger(SqsPocApplication.class);

//    @SqsListener(value = "${queue_name}", deletionPolicy = SqsMessageDeletionPolicy.NEVER)
//    public void queueListener1(@Headers Map<String, String> header, @Payload String message, Acknowledgment acknowledgment) throws InterruptedException, IOException {
//
////        for (Map.Entry<String,String> entry : header.entrySet())
////            System.out.println("Key = " + entry.getKey() +
////                    ", Value = " + entry.getValue());
//        //ReceiptHandle
//
////            try {
//
//               logger1.info("Listener 1: --------\n message from SQS Queue: {} \n Message Group ID: {} \n Thread ID: {}", message, header.get("MessageGroupId"), Thread.currentThread().getId());
//                addLogLines(message, header.get("MessageGroupId"));
//
//                acknowledgment.acknowledge();
////            sqsInst.deleteMessage(new DeleteMessageRequest(endpoint, header.get("ReceiptHandle")));
//
////            } catch (InterruptedException e) {
////                e.printStackTrace();
////            }
//
//
//
////        logger1.info("message from SQS Queue: {}", message);
////        logger1.info("Message Group ID: {}", header.get("MessageGroupId"));
//    }

    @SqsListener(value="${queue_name}", deletionPolicy = SqsMessageDeletionPolicy.ALWAYS)
    public void queueListener2(){
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(endpoint).withWaitTimeSeconds(20).withMaxNumberOfMessages(10);
        System.out.println("Listening for messages......");
        List<Message> sqsMessages = sqsInst.receiveMessage(receiveMessageRequest).getMessages();

        for(int i=0;i<sqsMessages.size(); i++)
        {
            System.out.println(sqsMessages.get(i).getBody() +" "+ sqsMessages.get(i).getAttributes().get("MessageGroupId"));
        }
    }




    private void addLogLines(String message, String messageGroupId) throws IOException {
        String str = message + " " + messageGroupId;
        BufferedWriter writer = new BufferedWriter(new FileWriter("SQSLogs.txt", true));
        writer.append("\n");
        writer.append(str);
        writer.close();
    }


    public static void main(String[] args) {
        SpringApplication.run(SqsPocApplication.class, args);
    }

}
