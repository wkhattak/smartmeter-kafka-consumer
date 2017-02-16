package com.khattak.bigdata.realtime.sensordataanalytics.smartmeter;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Timestamp;
import java.util.*;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//import org.apache.hadoop.io.IOUtils;
/*
 * Gets smart meter readings from Kafka topic, creates 5 min container files and then writes to HDFS 5,10,,,,55.csv 
 */
public class SmartMeterEventsConsumer extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(SmartMeterEventsConsumer.class);
    private static final String HDFS_PATH = "/hadoop/data/smartmeter/raw_readings/";

    public int run(String[] args) throws Exception {
        if (args.length != 4) 
        {
            System.out.println("Usage: SmartMeterEventsConsumer <broker list> <topic name> <consumer group id> <consumer unique id>");
            return 1;
        }
        
        LOG.debug("Using broker list:" + args[0] +", topic:" + args[1] +", consumer group id:" + args[2]);
               
        Properties props = new Properties();
        props.put("bootstrap.servers", args[0]); // localhost:9092
        props.put("group.id", args[2]); // Consumer group
        props.put("client.id", args[3]); // Client id for tracking
        props.put("enable.auto.commit", "true");// If true the consumer's offset will be periodically committed in the background.
        props.put("auto.commit.interval.ms", "1000"); //The frequency in milliseconds that the consumer offsets are auto-committed to Kafka if enable.auto.commit is set to true.
        props.put("session.timeout.ms", "30000"); // The timeout used to detect failures when using Kafka's group management facilities. When a consumer's heartbeat is not received within the session timeout, the broker will mark the consumer as failed and rebalance the group. Since heartbeats are sent only when poll() is invoked, a higher session timeout allows more time for message processing in the consumer's poll loop at the cost of a longer time to detect hard failures
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest"); //What to do when there is no initial offset in Kafka or if the current offset does not exist any more on the server (e.g. because that data has been deleted)

        String TOPIC = args[1]; //"smartmeter-readings"

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        try {
        	consumer.subscribe(Arrays.asList(TOPIC));
        	while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);//100 ms timeout
                LOG.info("Number of records received after polling: " + records.count());
                
                if (records.count() > 0) {
                	// get the file paths & data for each file that needs to be written to HDFS
                	Map<String,String> data = CreateFullFilePathAndData(records);
                	
	            	boolean fileWritten = WriteToHDFS(data);
	            	
	            	if (!fileWritten) LOG.info("HDFS file write ERROR!!!");

	                /*for (ConsumerRecord<String, String> record : records) {
	                	LOG.info("topic-partition >>>" + record.partition() + ", message number >>>" + record.offset() + ", key >>>" + record.key() +", value >>>" + parseMessage(record.value()));
	                }*/
                }
                Thread.sleep(15000);// 15 seconds
            }
        } catch (Exception e) {
        	consumer.close();
            e.printStackTrace();
        }
        
        return 0;
    }
    
    public static void main( String[] args ) throws Exception {
        int returnCode = ToolRunner.run(new SmartMeterEventsConsumer(), args);
        System.exit(returnCode);
    }

    /*
     * Creates a Map of file paths as key & content as the value
     */
    private Map<String,String> CreateFullFilePathAndData(ConsumerRecords<String, String> records) {
    	String fileName = "";
    	String previousFileName = "";// control variable for assigning a reading to the correct 5 min file
    	SmartmeterReading reading = null;
    	StringBuilder sbSameFileReadings = new StringBuilder();// container for keeping all readings for the same 5 min file
    	Map<String,String> results = new HashMap<String,String>(); // Map of filepath & content
    	
		for (ConsumerRecord<String, String> record : records) {
			// key has not been used which is the meter id
			
			//first reconstruct the reading
			reading = parseMessage(record.value());
			//Now get the file path based on reading's timestamp. Multiple readings will share the same file path depending upon which interval they fall in
	    	fileName  = CreateFilePathAndName(reading.timestamp);
	    	
	    	// Doesn't apply to first run of the loop
	    	// When current file name is not same as old then create an entry in the return map with the old file name as key and copy contents from the readings container for that file 
	    	if (previousFileName != null && !previousFileName.isEmpty() && !previousFileName.equals(fileName)) {
	    		results.put(previousFileName, sbSameFileReadings.toString());// after this we can deal with the current reading and file name
	    		sbSameFileReadings.setLength(0);// reset container for new file name readings
	    	}
	    	
	    	// add reading to container of all related readings (same interval/file path)
	    	sbSameFileReadings.append(reading.toString() + System.getProperty("line.separator"));
	    	previousFileName = fileName;// now keep a track of last file name as we don't want to write the next related reading to a different file 
		}
		
		// deal with the last file path
		if (fileName != null && !fileName.isEmpty() && sbSameFileReadings.length() > 0) results.put(fileName, sbSameFileReadings.toString()); 
		////sb.deleteCharAt(sb.lastIndexOf(System.getProperty("line.separator")));
		return results;
	}

    /*
     * Parses the string record retrieved from the Kafka topic into a SmartmeterReading object
     */
	private SmartmeterReading parseMessage(String message) {
    	String[] data = message.split("\\|");
		
		Timestamp timestamp = Timestamp.valueOf(cleanup(data[0]));
		String meterId = cleanup(data[1]);
		double fifteenSecondUsage = Double.parseDouble(cleanup(data[2]));
		String substationId = cleanup(data[3]);
		String city = cleanup(data[4]);
		double maxVoltage  = Double.parseDouble(cleanup(data[5]));
		double minVoltage  = Double.parseDouble(cleanup(data[6]));
		double powercutDuration  = Double.parseDouble(cleanup(data[7]));
		
		return new SmartmeterReading(timestamp,meterId,fifteenSecondUsage,substationId,city,maxVoltage,minVoltage,powercutDuration);
    }
    
    private String cleanup(String str) {
        if (str != null) {
            return str.trim().replace("\n", "").replace("\t", "");
        } 
        else {
            return str;
        }
        
    }

    /*
     * Writes data to HDFS.
     * Appends in case the file already exists as we don't want to get rid of old values
     */
    private boolean WriteToHDFS(Map<String,String> data) throws IOException{
         Path outputPath = null;
         Configuration conf = new Configuration();
         conf.addResource(new Path("/opt/hadoop/etc/hadoop/core-site.xml"));
         conf.addResource(new Path("/opt/hadoop/etc/hadoop/hdfs-site.xml"));
         conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
         conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
         //Configuration conf = getConf();
         FSDataOutputStream outputStream = null;
         BufferedWriter br = null;
         FileSystem fs = FileSystem.get(conf);
         //System.out.println("configured filesystem = " + conf.get("fs.defaultFS"));
         
         for (Map.Entry<String, String> datum : data.entrySet()) {
	         try {
		         outputPath = new Path(datum.getKey());
		         
		         if (fs.exists(outputPath)) {
		        	 outputStream = fs.append(outputPath);
		             LOG.info("Appending to file: " + outputPath);
		             //return false;
		         }
		         else {  
			         outputStream = fs.create(outputPath);
			         LOG.info("Writing to new file: " + outputPath);
			         //IOUtils.copyBytes(is, os, getConf(), false);
		         }
		         
		         br = new BufferedWriter( new OutputStreamWriter( outputStream, "UTF-8" ) );
		         br.write(datum.getValue());
	         }
	         catch (IOException exp){
	        	 System.err.println("Error:" + exp.getMessage());
	         }
	         finally {
	        	 try {
	        		  if (br != null) br.close();
	        		 if (outputStream != null) outputStream.close();
				} catch (IOException e) {
					e.printStackTrace();
					return false;
				}
	         }
         
         }
         return true;
    }

    /*
     * Creates a file path based on the passed in timestamp
     * Each reading is allocated to a 5 min interval
     * File paths are generated for 5 minute intervals
     * The logic works on the basis of checking the quotient of minutes/5. The quotient determines the file name
     * File name include 5,10,,,,,60.csv    
     */
    private String CreateFilePathAndName(Timestamp ts){
    	String filePath = "";
    	Calendar date = Calendar.getInstance();
    	date.setTimeInMillis(ts.getTime());
    	
    	String dir = HDFS_PATH + date.get(Calendar.YEAR) + "/" + (date.get(Calendar.MONTH) + 1) + "/" + date.get(Calendar.DAY_OF_MONTH) + "/" + date.get(Calendar.HOUR_OF_DAY);
        String fileName = "";
        
        switch (date.get(Calendar.MINUTE) / 5) {
        	case 0: 
        		fileName = "5.csv";
        		break;
        	case 1: 
        		fileName = "10.csv";
        		break;
        	case 2: 
        		fileName = "15.csv";
        		break;
        	case 3: 
        		fileName = "20.csv";
        		break;
        	case 4:
        		fileName = "25.csv";
        		break;
        	case 5: 
        		fileName = "30.csv";
        		break;
        	case 6: 
        		fileName = "35.csv";
        		break;
        	case 7: 
        		fileName = "40.csv";
        		break;
        	case 8:
        		fileName = "45.csv";
        		break;
        	case 9: 
        		fileName = "50.csv";
        		break;
        	case 10: 
        		fileName = "55.csv";
        		break;
        	case 11: 
        		fileName = "60.csv";
        		break;
        	default:
        		break;
        }
    	
        filePath = dir + "/" + fileName;
        
    	return filePath;
    }
    
    /*
     * inner class for representing the SmartmeterReading object
     */
    class SmartmeterReading {
    	Timestamp timestamp = null;
		String meterId = "";
		double fifteenSecondUsage = 0.0d;
		String substationId = "";
		String city = "";
		double maxVoltage  = 0.0d;
		double minVoltage  = 0.0d;
		double powercutDuration  = 0.0d;
		
		public SmartmeterReading(Timestamp timestamp, String meterId, double fifteenSecondUsage, String substationId, String city, double maxVoltage, double minVoltage, double powercutDuration){
			this.timestamp = timestamp;
			this.meterId = meterId;
			this.fifteenSecondUsage = fifteenSecondUsage;
			this.substationId = substationId;
			this.city = city;
			this.maxVoltage  = maxVoltage;
			this.minVoltage  = minVoltage;
			this.powercutDuration  = powercutDuration;
		}
		
		@Override
		public String toString(){
			
			return timestamp + "," + meterId + "," + String.format("%.4f", fifteenSecondUsage) + "," + substationId + "," + city + "," + String.format("%.2f", maxVoltage) + "," + String.format("%.2f", minVoltage) + "," + String.format("%.2f", powercutDuration);
		}
    }
}
