package com.jensonjo.SQSJavaClient.controller;

import java.util.List;
import java.util.Map;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import lombok.extern.java.Log;

/**
 * Created by jensonkakkattil on Apr, 2019.
 */
@RestController
@Log
public class TestController {
    /*
     * Create a new instance of the builder with all defaults (credentials
     * and region) set automatically. For more information, see
     * Creating Service Clients in the AWS SDK for Java Developer Guide.
     */
    final AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();

    @GetMapping(value = "/create-queue/{queueName}")
    public HttpEntity<Object> createQueue(@PathVariable(value = "queueName") String queueName,
                                          @RequestParam(name = "delete", required = false, defaultValue = "") String delete) {
        try {
            // Create a queue.
            log.info("Creating a new SQS queue called " + queueName);
            final CreateQueueRequest createQueueRequest =
                    new CreateQueueRequest(queueName);
            final String myQueueUrl = sqs.createQueue(createQueueRequest)
                    .getQueueUrl();

            // List all queues.
            log.info("Listing all queues in your account.\n");
            for (final String queueUrl : sqs.listQueues().getQueueUrls()) {
                log.info("  QueueUrl: " + queueUrl);
            }
            log.info("");

            // Send a message.
            log.info("Sending a message to " + queueName);
            sqs.sendMessage(new SendMessageRequest(myQueueUrl,
                    "This is my message text."));

            // Receive messages.
            log.info("Receiving messages from " + queueName);
            final ReceiveMessageRequest receiveMessageRequest =
                    new ReceiveMessageRequest(myQueueUrl);
            final List<Message> messages = sqs.receiveMessage(receiveMessageRequest)
                    .getMessages();
            for (final Message message : messages) {
                log.info("Message");
                log.info("  MessageId:     "
                        + message.getMessageId());
                log.info("  ReceiptHandle: "
                        + message.getReceiptHandle());
                log.info("  MD5OfBody:     "
                        + message.getMD5OfBody());
                log.info("  Body:          "
                        + message.getBody());
                for (final Map.Entry<String, String> entry : message.getAttributes()
                        .entrySet()) {
                    log.info("Attribute");
                    log.info("  Name:  " + entry
                            .getKey());
                    log.info("  Value: " + entry
                            .getValue());
                }
            }
            log.info(" ");

            if (delete.equalsIgnoreCase("yes") || delete.equalsIgnoreCase("true")) {
                // Delete the message.
                log.info("Deleting a message.\n");
                final String messageReceiptHandle = messages.get(0).getReceiptHandle();
                sqs.deleteMessage(new DeleteMessageRequest(myQueueUrl,
                        messageReceiptHandle));

                // Delete the queue.
                log.info("Deleting the test queue.\n");
                sqs.deleteQueue(new DeleteQueueRequest(myQueueUrl));
            }


        } catch (final AmazonServiceException ase) {
            log.info("Caught an AmazonServiceException, which means " +
                    "your request made it to Amazon SQS, but was " +
                    "rejected with an error response for some reason.");
            log.info("Error Message:    " + ase.getMessage());
            log.info("HTTP Status Code: " + ase.getStatusCode());
            log.info("AWS Error Code:   " + ase.getErrorCode());
            log.info("Error Type:       " + ase.getErrorType());
            log.info("Request ID:       " + ase.getRequestId());
        } catch (final AmazonClientException ace) {
            log.info("Caught an AmazonClientException, which means " +
                    "the client encountered a serious internal problem while " +
                    "trying to communicate with Amazon SQS, such as not " +
                    "being able to access the network.");
            log.info("Error Message: " + ace.getMessage());
            return new ResponseEntity<>("", HttpStatus.EXPECTATION_FAILED);
        }

        return new ResponseEntity<>(HttpStatus.OK);
    }
}
