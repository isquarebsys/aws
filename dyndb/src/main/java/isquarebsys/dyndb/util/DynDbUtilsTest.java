package isquarebsys.dyndb.util;

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
	}

}
