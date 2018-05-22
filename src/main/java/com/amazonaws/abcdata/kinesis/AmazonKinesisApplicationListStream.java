package com.amazonaws.abcdata.kinesis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.ABC.Contact;
import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;

import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.Shard;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;


public class AmazonKinesisApplicationListStream {
	private static AmazonKinesis kinesis;
	private static AmazonDynamoDBClient client;
	private static DynamoDB dynamodb;
	private static final Log LOG = LogFactory.getLog(AmazonKinesisRecordProducerSample.class);
	private static ProfileCredentialsProvider credentialsProvider = new ProfileCredentialsProvider();
	private static List<Contact> contact_list = new ArrayList<Contact>();
	private static List<Record> records = new ArrayList<Record>();
	private static boolean processedSuccessfully = false;
	private static ObjectMapper oMapper = new ObjectMapper();
	private static AmazonDynamoDB amazonDynamoDB;
	
	public static void init() {		
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
        client = new AmazonDynamoDBClient(credentialsProvider);
        dynamodb = new DynamoDB(client);;
        
	}
	
    public static void listStream(String myStreamName) {
    	String shardIterator;
    	
    	DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
    	describeStreamRequest.setStreamName( myStreamName );
    	List<Shard> shards = new ArrayList<Shard>();
    	String exclusiveStartShardId = null;
    	String sh=null;
    	try {
    	    describeStreamRequest.setExclusiveStartShardId( exclusiveStartShardId );
    	    DescribeStreamResult describeStreamResult = kinesis.describeStream(describeStreamRequest);
    	    shards.addAll( describeStreamResult.getStreamDescription().getShards() );
    	    System.out.println("Shard Size: " + shards.size());
    	    for(int i = 0; i < shards.size(); i++) {
	    	    sh = shards.get(i).getShardId();

	    	    /*-------*/
	    	    GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();    	
	        	getShardIteratorRequest.setStreamName(myStreamName);
	        	getShardIteratorRequest.setShardId(sh);
	        	getShardIteratorRequest.setShardIteratorType("TRIM_HORIZON");
	
	        	GetShardIteratorResult getShardIteratorResult = kinesis.getShardIterator(getShardIteratorRequest);
	        	shardIterator = getShardIteratorResult.getShardIterator();
	        	        	
	        	GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
	        	getRecordsRequest.setShardIterator(shardIterator);
	        	getRecordsRequest.setLimit(25);
	        	GetRecordsResult getRecordsResult = kinesis.getRecords(getRecordsRequest);
	        	records = getRecordsResult.getRecords();        	
	        	for (Record record : records) {            
	                try {
	                	processSingleRecord(record);
	                	processedSuccessfully = true;
	                }catch (Exception e) {
	                	System.out.println("Error occured retreiving the record");
	                	processedSuccessfully = false;
	                }
	        	}    	    
    	    }    	    
    	} catch(Exception e) {
    		
    	}
    	
        if(processedSuccessfully) {        	
        	Set<Contact> contact_list_noDuplicates = new LinkedHashSet<Contact>(contact_list);
        	System.out.println("Records list retrieved: " + records.size());
            for(Contact ct : contact_list_noDuplicates) {            	 
            	//System.out.println(ct.toString());
                 //client.setEndpoint("http://localhost:8000");            	
            	addToDynamodb(ct);
            	
            }
        }
    }
    
    private static void addToDynamodb(Contact ct)
    {
    	Map<String,AttributeValue> attributeValues = new HashMap<String, AttributeValue>();
    	attributeValues= oMapper.convertValue(ct, Map.class);    
   	 	//Table table = dynamoDB.getTable("spike_abc_kinesis_data");
   	 	try {
   	 		System.out.println("Going to add a record to dynamo.");
   	 		PutItemRequest putItemRequest = new PutItemRequest()
   	 				.withTableName("spike_abc_kinesis_data")
   	 				.withItem(attributeValues);
   	 		//you can check the put Item request details by un-commenting the lines below
   	 		/*Iterator it = attributeValues.entrySet().iterator();
   	 		while (it.hasNext()) {
   	 			Map.Entry pair = (Map.Entry)it.next();
   	 			System.out.println(pair.getKey() + " = " + pair.getValue());
   	 		}*/
   	 		//The statement adds to the table
   	 		amazonDynamoDB.putItem(putItemRequest);      	 		
   	 		System.out.println("Added a record to dynamo.  --  " + ct.toString());
   	 	}catch (Exception e) {
   	 		e.printStackTrace();
   	 	}    	
    }
    
    private static void processSingleRecord(Record record) {
        String data = null;
        try {
        	Contact ct = Contact.fromJsonAsBytes(record.getData().array());
        	
        	if (ct == null) {
        	    LOG.warn("Skipping record. Unable to parse record into Contact. Partition Key: " + record.getPartitionKey());
        	    return;
        	}
        	else {
        		if(contact_list.contains(ct)) {
            		System.out.println("duplicate");
            		}
        		if(!contact_list.contains(ct)) {
        		contact_list.add(ct);
        		}
        	}
        		
        } catch (NumberFormatException e) {
            LOG.info("Record does not match sample record format. Ignoring record with data; " + data);
        } catch (Exception e) {
            LOG.error("Malformed data: " + data, e);
        }
        	           
    }
    	
	public static void main(String[] args) {
		init();
        final String myStreamName = AmazonKinesisApplicationSample.SAMPLE_APPLICATION_STREAM_NAME;
        //final Integer myStreamSize = 1;
     // List all of my streams.
        try {
        listStream(myStreamName);
        }catch(Exception e) {
        	e.printStackTrace();
        }

	}

}
