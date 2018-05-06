package orm;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBIgnore;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

@DynamoDBTable(tableName = "CatalogItem")
public class CatalogItem {
	private Integer id;
	private String title;
	private String ISBN;
	private String someProp;

	@DynamoDBHashKey(attributeName = "Id")
	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	@DynamoDBAttribute(attributeName = "Title")
	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	@DynamoDBAttribute(attributeName = "ISBN")
	public String getISBN() {
		return ISBN;
	}

	public void setISBN(String ISBN) {
		this.ISBN = ISBN;
	}

	@DynamoDBIgnore
	public String getSomeProp() {
		return someProp;
	}

	public void setSomeProp(String someProp) {
		this.someProp = someProp;
	}
}
