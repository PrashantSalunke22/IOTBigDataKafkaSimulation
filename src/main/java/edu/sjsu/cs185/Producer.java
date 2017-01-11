package edu.sjsu.cs185; 

import com.google.common.io.Resources;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.AbstractRealDistribution;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Properties;

public class Producer {
	public static void main(String[] args) throws IOException {
		// error-check the command line
		if (args.length != 2) {
			System.err.println("usage: Producer <topic-name> <pump-id>");
			System.exit(1);
		}
		// parse the command line 
		String topic = args[0];
		int pumpId = Integer.parseInt(args[1]);

		// TODO: setup the normal distribution
		NormalDistribution nd=new NormalDistribution(0,0.2);
		
		// setup the producer
        	KafkaProducer<String, String> producer=null;
        	try (InputStream props = Resources.getResource("producer.props").openStream()) {
            		Properties properties = new Properties();
            		properties.load(props);
            		producer = new KafkaProducer<>(properties);

        		while (true) {
                		// TODO: get current time stamp
        				Date date = new Date();
        				long timestamp = date.getTime();
                		// TODO: get vibration delta 
        				double vibration_data= nd.sample();                		
        				// TODO: create message 
        				String message = pumpId+","+timestamp+","+vibration_data;
                        ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topic, message);

                		// TODO: publish message to topic 
        				producer.send(rec);
                		// TODO: flush producer
        				producer.flush();
        				Thread.sleep(1000);
            		}
		}
        	catch (Exception e) {
            		System.err.println(e.toString());
		}
        	finally {
            		producer.close();
        	}

    	}
}
