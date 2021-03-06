package com.amazonaws.abcdata.kinesis;
/*
 * Copyright 2012-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

import java.nio.ByteBuffer;
import com.ABC.Contact;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.abcdata.kinesis.ContactDataGenerator;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ListStreamsRequest;
import com.amazonaws.services.kinesis.model.ListStreamsResult;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.StreamDescription;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public class AmazonKinesisRecordProducerSample {

    /*
     * Before running the code:
     *      Fill in your AWS access credentials in the provided credentials
     *      file template, and be sure to move the file to the default location
     *      (/home/singhs/.aws/credentials) where the sample code will load the
     *      credentials from.
     *      https://console.aws.amazon.com/iam/home?#security_credential
     *
     * WARNING:
     *      To avoid accidental leakage of your credentials, DO NOT keep
     *      the credentials file in your source directory.
     */

    private static AmazonKinesis kinesis;
    private static final Log LOG = LogFactory.getLog(AmazonKinesisRecordProducerSample.class);

    private static void init() throws Exception {
        /*
         * The ProfileCredentialsProvider will return your [default]
         * credential profile by reading from the credentials file located at
         * (/home/singhs/.aws/credentials).
         */
        ProfileCredentialsProvider credentialsProvider = new ProfileCredentialsProvider();
        try {
            credentialsProvider.getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (/home/singhs/.aws/credentials), and is in valid format.",
                    e);
        }

        kinesis = AmazonKinesisClientBuilder.standard()
            .withCredentials(credentialsProvider)
            .withRegion("us-east-2")
            .build();
    }
    
    public static void describeStream(String myStreamName, Integer myStreamSize) {
    	 // Describe the stream and check if it exists.
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest().withStreamName(myStreamName);
        try {
            StreamDescription streamDescription = kinesis.describeStream(describeStreamRequest).getStreamDescription();
            System.out.printf("Stream %s has a status of %s.\n", myStreamName, streamDescription.getStreamStatus());

            if ("DELETING".equals(streamDescription.getStreamStatus())) {
                System.out.println("Stream is being deleted. This sample will now exit.");
                System.exit(0);
            }

            // Wait for the stream to become active if it is not yet ACTIVE.
            if (!"ACTIVE".equals(streamDescription.getStreamStatus())) {
                waitForStreamToBecomeAvailable(myStreamName);
            }
        } catch (Exception ex) {
            System.out.printf("Stream %s does not exist. Creating it now.\n", myStreamName);

            // Create a stream. The number of shards determines the provisioned throughput.
            CreateStreamRequest createStreamRequest = new CreateStreamRequest();
            createStreamRequest.setStreamName(myStreamName);
            createStreamRequest.setShardCount(myStreamSize);            
            kinesis.createStream(createStreamRequest);
            // The stream is now being created. Wait for it to become active.
            try {
				waitForStreamToBecomeAvailable(myStreamName);
			} catch (InterruptedException e) {
				System.out.printf("Stream %s does not exist. Creating it now.\n", myStreamName);
				CreateStreamRequest createStreamRequest1 = new CreateStreamRequest();
	            createStreamRequest1.setStreamName(myStreamName);
	            createStreamRequest1.setShardCount(myStreamSize);
	            kinesis.createStream(createStreamRequest1);
			}
        }
    }
    
    public static void listStream(String myStreamName) {
    	ListStreamsRequest listStreamsRequest = new ListStreamsRequest();
        listStreamsRequest.setLimit(10);
        ListStreamsResult listStreamsResult = kinesis.listStreams(listStreamsRequest);
        List<String> streamNames = listStreamsResult.getStreamNames();
        while (listStreamsResult.isHasMoreStreams()) {
            if (streamNames.size() > 0) {
                listStreamsRequest.setExclusiveStartStreamName(streamNames.get(streamNames.size() - 1));
            }

            listStreamsResult = kinesis.listStreams(listStreamsRequest);
            streamNames.addAll(listStreamsResult.getStreamNames());
        }
        // Print all of my streams.
        System.out.println("List of my streams: ");
        for (int i = 0; i < streamNames.size(); i++) {
            System.out.println("\t- " + streamNames.get(i));
        }

    }

    public static void main(String[] args) throws Exception {
        init();

        final String myStreamName = AmazonKinesisApplicationSample.SAMPLE_APPLICATION_STREAM_NAME;
        final Integer myStreamSize = 1;

        // Describe the stream and check if it exists.
        describeStream(myStreamName,myStreamSize);        
        // List all of my streams.
        listStream(myStreamName);
        List<Contact> contact_list = ContactDataGenerator.addContact(10, 1);
        System.out.printf("Total records: %s", contact_list.size());
        System.out.printf("Putting records in stream : %s until this application is stopped...\n", myStreamName);
        addRecord(contact_list,kinesis,myStreamName);
        
    }
    private static void addRecord(List<Contact> contact_list,  AmazonKinesis kinesisClient, String streamName) {
    	for(Contact ct : contact_list) {	    	
	        byte[] bytes = ct.toJsonAsBytes();	        
	        if (bytes == null) {
	            LOG.warn("Could not get JSON bytes for contact");
	            return;
	        }
	        LOG.info("Putting Contact: " + ct.toString());
	        PutRecordRequest putRecord = new PutRecordRequest();
	        putRecord.setStreamName(streamName);
	        putRecord.setPartitionKey(Integer.toString(ct.getContactId()));   
	        putRecord.setData(ByteBuffer.wrap(bytes));
	
	        try {
	            kinesisClient.putRecord(putRecord);        
	        } catch (AmazonClientException ex) {
	            LOG.warn("Error sending record to Amazon Kinesis.", ex);
	        }
    	}
    }
    
    	
    private static void waitForStreamToBecomeAvailable(String myStreamName) throws InterruptedException {
        System.out.printf("Waiting for %s to become ACTIVE...\n", myStreamName);

        long startTime = System.currentTimeMillis();
        long endTime = startTime + TimeUnit.MINUTES.toMillis(10);
        while (System.currentTimeMillis() < endTime) {
            Thread.sleep(TimeUnit.SECONDS.toMillis(20));

            try {
                DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
                describeStreamRequest.setStreamName(myStreamName);
                // ask for no more than 10 shards at a time -- this is an optional parameter
                describeStreamRequest.setLimit(10);
                DescribeStreamResult describeStreamResponse = kinesis.describeStream(describeStreamRequest);

                String streamStatus = describeStreamResponse.getStreamDescription().getStreamStatus();
                System.out.printf("\t- current state: %s\n", streamStatus);
                if ("ACTIVE".equals(streamStatus)) {
                    return;
                }
            } catch (ResourceNotFoundException ex) {
                // ResourceNotFound means the stream doesn't exist yet,
                // so ignore this error and just keep polling.
            } catch (AmazonServiceException ase) {
                throw ase;
            }
        }

        throw new RuntimeException(String.format("Stream %s never became active", myStreamName));
    }
}
