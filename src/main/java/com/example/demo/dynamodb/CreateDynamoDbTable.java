
package com.example.demo.dynamodb;

import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.CreateTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter;

public class CreateDynamoDbTable {

	public static void main(String[] args) {

		final String USAGE = "\n" +
                "Usage:\n" +
                "    CreateDynamoDbTable <tableName> <key>\n\n" +
                "Where:\n" +
                "    tableName - the Amazon DynamoDB table to create (for example, Music3).\n\n" +
                "    key - the key for the Amazon DynamoDB table (for example, Artist).\n" +
                "Example:\n" +
                "    Music3 Artist \n";
		
		if (args.length != 2) {
            System.out.println(USAGE);
            System.exit(1);
        }
		
		String tableName = args[0];
        String key = args[1];
        System.out.format(
                "Creating an Amazon DynamoDB table \"%s\" with a simple primary key: \"Name\".\n",
                tableName);

        Region region = Region.AP_SOUTHEAST_2;
        DynamoDbClient ddb = DynamoDbClient.builder()
                .region(region)
                .build();
        
        String result = createTable(ddb, tableName, key);
        System.out.println("New table is "+result);
        ddb.close();
        
	}
	
	
	public static String createTable(DynamoDbClient ddb, String tableName, String key) {
	
		
		DynamoDbWaiter dbWaiter = ddb.waiter();
		CreateTableRequest request = CreateTableRequest.builder()
				.attributeDefinitions(AttributeDefinition.builder()
						.attributeName(key)
						.attributeType(ScalarAttributeType.S)
						.build())
				.keySchema(KeySchemaElement.builder()
						.attributeName(key)
						.keyType(KeyType.HASH)
						.build())
				.provisionedThroughput(ProvisionedThroughput.builder()
						.readCapacityUnits(new Long(10))
						.writeCapacityUnits(new Long(10))
						.build())
				.tableName(tableName)
				.build();
		
		
		String newTable = "";
		try {
			CreateTableResponse response = ddb.createTable(request);
			DescribeTableRequest tableRequest = DescribeTableRequest.builder()
					.tableName(tableName)
					.build();
			
			//wait until the Amazon DynamoDB table is created
			WaiterResponse<DescribeTableResponse> waiterResponse  = dbWaiter.waitUntilTableExists(tableRequest);
			waiterResponse.matched().response().ifPresent(System.out::println);
			
			
			newTable = response.tableDescription().tableName();
			return newTable;
			
			
		} catch (DynamoDbException e) {
			System.err.println(e.getMessage());
			System.exit(1);
		}
		
		return "";
	}

}
