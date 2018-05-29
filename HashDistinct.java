import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Pattern;
import java.util.*;
import java.util.regex.Matcher;
import java.util.Enumeration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

/**
 *complie HDFSTest.java
 *
 * javac HDFSTest.java 
 *
 *execute HDFSTest.java
 *
 * java HDFSTest  
 * 
 */

public class HashDistinct {
	/**
 	 * 
	 */    
    public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException, URISyntaxException{
        /**
	 * 
	 */
	String whichFile = args[0];
	String regex0 = "\\/.*\\.tbl";
	Pattern pat0 = Pattern.compile(regex0);
	Matcher matcher0 = pat0.matcher(whichFile);
	String File = new String();
	while(matcher0.find()){
	//String file0 = matcher0.group();
	//System.out.println(file0);
	File = matcher0.group();
	}
	String file= "hdfs://localhost:9000" + File ;
	
	String regex = "\\|";
	String regex1 = "\\,";
	String select1 = args[1];
	int row = 0;//which row to choose;
	String distinct = args[2];
	
	//to analyze the select
	String[] sle1 = select1.split(regex1);	
	String regEx1 = "\\d+";
	Pattern pat1 = Pattern.compile(regEx1);
	Matcher matcher1 = pat1.matcher(sle1[0]);
	while(matcher1.find()){
	row = Integer.parseInt(matcher1.group());//which row,must be this form
	}
	String operation = sle1[1];//which operation
	double num =Double.parseDouble(sle1[2]);//condition number
	
	//to analyze the distinct
	String[] dis = distinct.split(regex1);
	int i=0;
	int[] row_number = new int[dis.length];//to store which row to choose
	for(String ele:dis){
	    
	    Matcher matcher2 = pat1.matcher(ele);//to match numbers in distinct
	    while(matcher2.find()){
	    row_number[i] = Integer.parseInt(matcher2.group());
	    }	//change to int 
            i++;
	}	
	// to configurate Hbase
	
	Configuration configuration = HBaseConfiguration.create();
        HBaseAdmin hAdmin = new HBaseAdmin(configuration);
	
	String tableName = "Result";
	
	if(hAdmin.tableExists(tableName)){
		hAdmin.disableTable(tableName);
		hAdmin.deleteTable(tableName);
	}
	
	HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));

	HColumnDescriptor cf = new HColumnDescriptor("res");
	htd.addFamily(cf);

	//Configuration configuration = HBaseConfiguration.create();
	//HBaseAdmin hAdmin = new HBaseAdmin(configuration);

	hAdmin.createTable(htd);
	hAdmin.close();
		
	HTable table = new HTable(configuration,tableName);
	

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(file), conf);
        Path path = new Path(file);
        FSDataInputStream in_stream = fs.open(path);

        BufferedReader in = new BufferedReader(new InputStreamReader(in_stream));
        String s;
	Hashtable H = new Hashtable();
	        
	while ((s=in.readLine())!=null) {
	     String[] arr = s.split(regex);
	   	StringBuffer Key = new StringBuffer("");
	     switch(operation){
	     case "gt":
	     if(Double.parseDouble(arr[row])>num){
		for(int j:row_number){    //Travesal to construct a hashtable
                Key = Key.append(arr[j] + "|");
                }
                H.put(Key.toString(),0);
	
	     }
	     break;
	     case "ge":
             if(Double.parseDouble(arr[row])>num){
                for(int j:row_number){    //Travesal to construct a hashtable
                Key = Key.append(arr[j] + "|");
                }
                H.put(Key.toString(),0);
	
             }
	     break;
	     case "eq":
             if(Double.parseDouble(arr[row])==num){
                for(int j:row_number){    //Travesal to construct a hashtable
                Key = Key.append(arr[j] + "|");
                }
                H.put(Key.toString(),0);
	        
             }
	     break;
	     case "ne":
             if(Double.parseDouble(arr[row]) != num){
                for(int j:row_number){    //Travesal to construct a hashtable
                Key = Key.append(arr[j] + "|");
                }
                H.put(Key.toString(),0);

             }
	     break;
	     case "le":
             if(Double.parseDouble(arr[row])<=num){
                for(int j:row_number){    //Travesal to construct a hashtable
                Key = Key.append(arr[j] + "|");
                }
                H.put(Key.toString(),0);

             }
	     break;
	     case "lt":
             if(Double.parseDouble(arr[row])<num){
                for(int j:row_number){    //Travesal to construct a hashtable
                Key = Key.append(arr[j] + "|");
                }
                H.put(Key.toString(),0);

             }
	     break;
	     }
     
        }

	//Travesal Hashtable to get result
	Enumeration<String> e = H.keys();
	Integer n = 0;
	while(e.hasMoreElements()){
	    String key = e.nextElement();
	    // System.out.println(key);
	    String[] Rs = key.split("\\|");

	    Put put = new Put(n.toString().getBytes());
	    int k = 0;
	    for(int j:row_number){
		put.add("res".getBytes(),("R"+j).getBytes(),Rs[k].toString().getBytes());	
		table.put(put);	
		k++;
		}
	    n++;
	}
	table.close();
	//System.out.println(n);
	
        in.close();

        fs.close();
    }
}
