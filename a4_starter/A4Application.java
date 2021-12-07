import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;



public class A4Application {

	public static void main(String[] args) throws Exception {
		// do not modify the structure of the command line
		String bootstrapServers = args[0];
		String appName = args[1];
		String studentTopic = args[2];
		String classroomTopic = args[3];
		String outputTopic = args[4];
		String stateStoreDir = args[5];

		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, appName);
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.STATE_DIR_CONFIG, stateStoreDir);
		props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

		// add code here if you need any additional configuration options

		StreamsBuilder builder = new StreamsBuilder();

		// add code here
		// 
		KStream<String, String> student = builder.stream(studentTopic);
		KStream<String, String> classroom = builder.stream(classroomTopic);

		// <student_id, classroom>
		KTable<String, String> student_classroom = student.groupByKey().reduce((aValue,newValue) -> newValue);


		//<classroom, occupancy> 
		KTable<String, Long> classroom_occupancy = student_classroom.groupByKey().reduce((student_id,room_id) -> new KeyValue<>(room_id, student_id)).count();


		//<classroom, capacity>
		KTable<String, String> classroom_capacity = classroom.groupByKey().reduce((aValue,newValue) -> newValue);

		// join two occupancy and capacity together
		KTable<String, String> current_classroom = classroom_occupancy.join(classroom_capacity, (occupancy, capacity) -> {
			return occupancy.toString() + "," + capacity.toString();
		});

		// new output table

		KTable<String, String> finalOutput = current_classroom.toStream().groupByKey().aggreate(() -> null, (key, newValue, oldValue) -> {
			int newOccupancy = Integer.parseInt(newValue.split(",")[0]);
			int newCapacity = Integer.parseInt(newValue.split(",")[1]);
			if (newOccupancy > newCapacity) {   // more than capacity
				return String.valueOf(newOccupancy);  
			} else { // equal to capacity
				if(StringUtils.isNumeric(oldValue)) {
					return "OK";
				} else {
					return null;
				}
			}
		});

		Serde<String> sSerde = Serdes.String();
		finalOutput.toStream().filter((key,value) -> value != null).to(outputTopic, Produced.with(sSerde, sSerde));

		// ...to(outputTopic);

		KafkaStreams streams = new KafkaStreams(builder.build(), props);

		// this line initiates processing
		streams.start();

		// shutdown hook for Ctrl+C
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}
}
