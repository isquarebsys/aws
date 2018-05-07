package isquarebsys.dyndb.util;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.amazonaws.services.dynamodbv2.util.TableUtils.TableNeverTransitionedToStateException;

public class DynDbUtils {
	private static AmazonDynamoDB dynamoDB = null;

	public DynDbUtils() {
		getLocalInstance();
	}

	public static AmazonDynamoDB getLocalInstance() {
		if (dynamoDB == null) {
			ProfileCredentialsProvider credentialsProvider = new ProfileCredentialsProvider();
			try {
				credentialsProvider.getCredentials();
			} catch (Exception e) {
				throw new AmazonClientException("Cannot load the credentials from the credential profiles file. "
						+ "Please make sure that your credentials file is at the correct "
						+ "location (C:\\Users\\user\\.aws\\credentials), and is in valid format.", e);
			}
			// dynamoDB = AmazonDynamoDBClientBuilder.standard()
			// .withCredentials(credentialsProvider)
			// .withRegion("ap-south-1")
			// .build();

			dynamoDB = AmazonDynamoDBClientBuilder.standard()
					.withEndpointConfiguration(new EndpointConfiguration("http://localhost:8000", "ap-south-1"))
					.build();
		}
		return dynamoDB;
	}

	/**
	 * Creates Table with HashKey
	 * 
	 * @param tableName
	 * @param hashKeyName
	 */
	public String createTable(String tableName, String hashKeyName) {
		String result;
		try {
			AmazonDynamoDB dynamoDB = getLocalInstance();
			// Create a table with a primary hash key named 'name', which holds a string
			CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName)
					.withKeySchema(new KeySchemaElement().withAttributeName(hashKeyName).withKeyType(KeyType.HASH))
					.withAttributeDefinitions(new AttributeDefinition().withAttributeName(hashKeyName)
							.withAttributeType(ScalarAttributeType.S))
					.withProvisionedThroughput(
							new ProvisionedThroughput().withReadCapacityUnits(1L).withWriteCapacityUnits(1L));

			// Create table if it does not exist yet
			TableUtils.createTableIfNotExists(dynamoDB, createTableRequest);
			// wait for the table to move into ACTIVE state

			TableUtils.waitUntilActive(dynamoDB, tableName);
			result="Table Created Successfully";
		} catch (TableNeverTransitionedToStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			result="Exception: "+e.toString();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			result="Exception: "+e.toString();
		}
		return result;
	}

	public void describeTable(String tableName) {
		DescribeTableRequest describeTableRequest = new DescribeTableRequest().withTableName(tableName);
		TableDescription tableDescription = dynamoDB.describeTable(describeTableRequest).getTable();
		System.out.println("Table Description: " + tableDescription);
	}

	public ScanResult scanResult(String tableName) {
		HashMap<String, Condition> scanFilter = new HashMap<String, Condition>();
		Condition condition = new Condition().withComparisonOperator(ComparisonOperator.GT.toString())
				.withAttributeValueList(new AttributeValue().withN("1985"));
		scanFilter.put("year", condition);
		ScanRequest scanRequest = new ScanRequest(tableName).withScanFilter(scanFilter);
		ScanResult scanResult = dynamoDB.scan(scanRequest);
		return scanResult;
	}
	
	public void addItem(String tableName,Map<String, AttributeValue> item) {
		PutItemRequest putItemRequest = new PutItemRequest(tableName, item);
        PutItemResult putItemResult = dynamoDB.putItem(putItemRequest);
        System.out.println("Result: " + putItemResult);
	}
}
