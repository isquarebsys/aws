package orm;

import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;

public class CatalogItemMain {
	
	 /*
     * Before running the code:
     *      Fill in your AWS access credentials in the provided credentials
     *      file template, and be sure to move the file to the default location
     *      (C:\\Users\\user\\.aws\\credentials) where the sample code will load the
     *      credentials from.
     *      https://console.aws.amazon.com/iam/home?#security_credential
     *
     * WARNING:
     *      To avoid accidental leakage of your credentials, DO NOT keep
     *      the credentials file in your source directory.
     */

    static AmazonDynamoDB dynamoDB;

    /**
     * The only information needed to create a client are security credentials
     * consisting of the AWS Access Key ID and Secret Access Key. All other
     * configuration, such as the service endpoints, are performed
     * automatically. Client parameters, such as proxies, can be specified in an
     * optional ClientConfiguration object when constructing a client.
     *
     * @see com.amazonaws.auth.BasicAWSCredentials
     * @see com.amazonaws.auth.ProfilesConfigFile
     * @see com.amazonaws.ClientConfiguration
     */
    private static void init() {
        /*
         * The ProfileCredentialsProvider will return your [default]
         * credential profile by reading from the credentials file located at
         * (C:\\Users\\user\\.aws\\credentials).
         */
        ProfileCredentialsProvider credentialsProvider = new ProfileCredentialsProvider();
        try {
            credentialsProvider.getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (C:\\Users\\user\\.aws\\credentials), and is in valid format.",
                    e);
        }
        dynamoDB = AmazonDynamoDBClientBuilder.standard()
            .withCredentials(credentialsProvider)
            .withRegion("ap-south-1")
            .build();
        
//        dynamoDB = AmazonDynamoDBClientBuilder.standard().withEndpointConfiguration(
//        		new EndpointConfiguration("http://localhost:8000", "ap-south-1")).build();
    }

	public static void main(String[] args) {
		init();
		DynamoDBMapper mapper = new DynamoDBMapper(dynamoDB);

		CatalogItem item = new CatalogItem();
		item.setId(102);
		item.setTitle("Book 102 Title");
		item.setISBN("222-2222222222");
		item.setSomeProp("Test");
		mapper.save(item);  
		
		
		// Querying the Item
		CatalogItem partitionKey = new CatalogItem();
		partitionKey.setId(102);
		DynamoDBQueryExpression<CatalogItem> queryExpression = new DynamoDBQueryExpression<CatalogItem>()
		    .withHashKeyValues(partitionKey);

		List<CatalogItem> itemList = mapper.query(CatalogItem.class, queryExpression);

		for (int i = 0; i < itemList.size(); i++) {
		    System.out.println(itemList.get(i).getTitle());
		}
	}

}
