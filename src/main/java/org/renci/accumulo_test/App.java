package org.renci.accumulo_test;

/**
 * Hello world!
 *
	public class App
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
    }
}
**/

import org.apache.accumulo.core.client.AccumuloException;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONObject;
import org.apache.hadoop.io.Text;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class App {
  public static void main (String[] args) throws AccumuloException, AccumuloSecurityException,TableNotFoundException, TableExistsException, ClassNotFoundException, IOException{
        // Constants
        String instanceName = "exogeni";
        String zooServers = "172.16.100.5,172.16.100.4,172.16.100.1"; // Provide list of zookeeper server here. For example, localhost:2181
        String userName = "root"; // Provide username
        String password = "secret"; // Provide password
        // Connect
        //System.out.println("hi");


        Instance inst = new ZooKeeperInstance(instanceName,zooServers);
	       System.out.println("checkpoint1");
        Connector conn = inst.getConnector(userName, password);
	       System.out.println("checkpoint2");
       // Create our new table

        String tableName = "trace";
        TableOperations ops = conn.tableOperations();
         if (ops.exists(tableName)) {
            ops.delete(tableName);
        }
        ops.create(tableName);

        // Use batch writer to write demo data
        //String tableName = "trace";
        BatchWriter bw = conn.createBatchWriter(tableName,1000000, 60000, 2);

        // set values
        Text rowID = new Text("id0002");
        Text colFam = new Text("colFam");
        Text colQual = new Text("colQual");
        final String secretId = "secretId";
        ColumnVisibility colVis = new ColumnVisibility(secretId);
        // set value
        Value value = new Value("some-value".getBytes());
        // create new mutation and add rowID, colFam, colQual, and value
        Mutation mutation = new Mutation(rowID);

        mutation.put(colFam, colQual, colVis, value);
        // add the mutation to the batch writer
        bw.addMutation(mutation);
        // close the batch writer
        bw.close();

        System.out.println("checkpoint3");


        /*	    Map<String, Value> out = new HashMap<>();
	    out = readOneRow( conn,  "trace", new Text("id0001"), new Text("hero"), new Text("alias2"), "secretId");
	    for (Map.Entry<String, Value> entry : out.entrySet()) {
	    		System.out.println(entry.getValue());
	    }*/

        Scanner scan = conn.createScanner(tableName, new Authorizations());
        scan.setRange(new Range("id0002", "id0002"));
        Map<String, Value> out = new HashMap<String, Value>();
	    out = readOneRow( conn,  "trace", new Text("id0001"), new Text("colFam"), new Text("colQual"), "secretId");
	    for (Map.Entry<String, Value> entry : out.entrySet()) {
	    		System.out.println(entry.getValue());
	    }
	    
	    
	    
	    
	    
	    
	    
	    //Serialize and add to Accumulo
	    Authorizations auths = new Authorizations("secretId");
	    
	    
	    String[] valueArr = {"false", "writeToken", "scopeValue"};
		byte[] serializedByteArray = null;
		try {
			serializedByteArray = serialize(valueArr);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
		org.apache.accumulo.core.data.Value accumuloValue = new org.apache.accumulo.core.data.Value(serializedByteArray);
		Mutation mutation4 = new Mutation("id0002");
		mutation4.put("hero","alias2", accumuloValue);
		mutation4.put("hero","name2", colVis, "Dick Grayson2");
		mutation4.put("hero","wearsCape2?", colVis, "true2");
		
		
		try (BatchWriter writer = conn.createBatchWriter("trace", new BatchWriterConfig())) {
			writer.addMutation(mutation4);
		}
	    
		
		//Read one row and get deserialized
		Map<String, Value> scopeOut = new HashMap<>();
		scopeOut = readOneRow( conn,  "trace", new Text("id0002"), new Text("hero"), new Text("alias2"), "secretId");
		System.out.println(scopeOut.size());
	    for (Map.Entry<String, Value> entry : scopeOut.entrySet()) {
	    		Value arr = entry.getValue();
	    		String[] de = (String[]) deserialize(arr.get());
	    		System.out.println("ATTENTION!!!");
	    		for (String v : de)
	    			System.out.println(v);
	    		System.out.println("ATTENTION!!!");
	    }
		
		
		
	    
	    
	    
	    
		
		
	    conn.securityOperations().changeUserAuthorizations("root", auths);
		Scanner scanner = conn.createScanner("trace", auths);
		//scanner.setBatchSize(scanOpts.scanBatchSize);
		scanner.setRange(new Range("id0001"));
		scanner.fetchColumn(new Text("hero"), new Text("name"));
		for (Map.Entry<Key, Value> entry : scanner) {
	    		Key key = entry.getKey();
			Value value1 = entry.getValue();
	        	if (value1 == null) {
	        		System.out.println("value doesn't exist");
	        	}
	    		System.out.printf("Key : %-50s  Value : %s\n", key, value1);
	    }
        /*Iterator<Map.Entry<Key,Value>> iterator = scan.iterator();
        while (iterator.hasNext()) {
        		Map.Entry<Key, Value> entry = iterator.next();
        		Key key = entry.getKey();
        		Value gotValue = entry.getValue();
        		System.out.println(key + " ==> " + gotValue);
        }*/
    }

  	public static Map<String, Value> readOneRow(Connector conn, String tableName, Text rowID, Text colFam, Text colQual, String visibility) throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
		JSONObject output = new JSONObject();
		//ScannerOpts scanOpts = new ScannerOpts();
		// Create a scanner
		Authorizations auths = new Authorizations(visibility);
		conn.securityOperations().changeUserAuthorizations("root", auths);
	    Scanner scanner = conn.createScanner(tableName, auths);
	    //scanner.setBatchSize(scanOpts.scanBatchSize);
	    // Say start key is the one with key of row
	    // and end key is the one that immediately follows the row
	    scanner.setRange(new Range(rowID));
	    scanner.fetchColumn(colFam, colQual);
	    Map<String, Value> out = new HashMap<String, Value>();
	    for (Map.Entry<Key, Value> entry : scanner) {
	    		Key key = entry.getKey();
			Value value = entry.getValue();

			out.put(key.toString(), value);

	    		System.out.printf("Key : %-50s  Value : %s\n", entry.getKey(), entry.getValue());
	    }
	    scanner.close();
	    return out;
	}
  	
  	public static byte[] serialize(Object obj) throws IOException {
	    ByteArrayOutputStream out = new ByteArrayOutputStream();
	    ObjectOutputStream os = new ObjectOutputStream(out);
	    os.writeObject(obj);
	    return out.toByteArray();
	}
	
	public static Object deserialize(byte[] data) throws IOException, ClassNotFoundException {
	    ByteArrayInputStream in = new ByteArrayInputStream(data);
	    ObjectInputStream is = new ObjectInputStream(in);
	    return is.readObject();
	}
}
