package com.programmerid.santaclauslambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent.DynamodbStreamRecord;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sns.model.PublishRequest;
import lombok.extern.slf4j.Slf4j;

import static java.util.Objects.isNull;

@Slf4j
public class SantaClausDynamoDbEventRequestHandler implements RequestHandler<DynamodbEvent, Void> {

    public static final String SANTA_CLAUS_LETTERS_SNS_TOPIC_ARN = "SANTA_CLAUS_LETTERS_TOPIC_SNS_ARN";


    @Override
    public Void handleRequest(DynamodbEvent event, Context context) {
        final String topicArn = System.getenv(SANTA_CLAUS_LETTERS_SNS_TOPIC_ARN);
        if (isNull(topicArn)) {
            log.error("{} property is not set ", SANTA_CLAUS_LETTERS_SNS_TOPIC_ARN);
            return null;
        } else {
            log.info("{} is set to {}", SANTA_CLAUS_LETTERS_SNS_TOPIC_ARN, topicArn);
        }

        AmazonSNS client = AmazonSNSClientBuilder.defaultClient();

        for (DynamodbStreamRecord record : event.getRecords()) {
            log.info(record.getEventID());
            log.info(record.getEventName());
            log.info(record.getDynamodb().toString());
            client.publish(new PublishRequest(topicArn, record.toString()));
        }
        log.info("Successfully processed " + event.getRecords().size() + " records.");
        return null;
    }
}