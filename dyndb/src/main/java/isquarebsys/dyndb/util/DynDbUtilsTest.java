package isquarebsys.dyndb.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

public class DynDbUtilsTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String tableName = "CatalogItem";
		DynDbUtils dbUtil=new DynDbUtils();
		System.out.println(dbUtil.createTable(tableName, "id"));
		dbUtil.describeTable(tableName);
		ScanResult scanResult=dbUtil.scanResult(tableName);
		System.out.println(scanResult);
		
		Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
//		item.put("id",new AttributeValue().withN("1"));
        item.put("name", new AttributeValue("Vijay"));
        item.put("year", new AttributeValue().withN("1976"));
        item.put("rating", new AttributeValue("1"));
        dbUtil.addItem(tableName, item);
	}

}
