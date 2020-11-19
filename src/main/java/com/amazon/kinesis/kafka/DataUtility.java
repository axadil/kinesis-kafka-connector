package com.amazon.kinesis.kafka;

import java.io.UnsupportedEncodingException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import com.amazonaws.services.kinesisfirehose.model.Record;

public class DataUtility {

	public static ByteBuffer parseValue(Schema schema, Object value) {
            if (value == null) {
                return null;
            }
            if (value instanceof Number) {
                ByteBuffer longBuf = ByteBuffer.allocate(8);
                longBuf.putLong((Long) value);
                return longBuf;
            }
            if (value instanceof Boolean) {
                ByteBuffer boolBuffer = ByteBuffer.allocate(1);
                boolBuffer.put((byte) ((Boolean) value ? 1 : 0));
                return boolBuffer;
            }
            if (value instanceof String) {
                try {
                    return ByteBuffer.wrap(((String) value).getBytes("UTF-8"));
                } catch (UnsupportedEncodingException e) {
                    System.out.println("Message cannot be translated:" + e.getLocalizedMessage());
                }
            }
            if(value instanceof HashMap) {		
                try {
                    String json =  new ObjectMapper().writeValueAsString(value); 
                    return parseValue(json);
                } catch (JsonProcessingException e) {
                    System.out.println("JSON couldn't be processed" + e.getLocalizedMessage());
                }
            }
            if (value instanceof byte[] || value instanceof ByteBuffer) {
                if (value instanceof byte[])
                    return ByteBuffer.wrap((byte[]) value);
                else if (value instanceof ByteBuffer)
                    return (ByteBuffer) value;
            }
            System.out.println("Unsupported value type: " + value.getClass());
            return null;
        }

	/**
	 * Converts Kafka record into Kinesis record
	 * 
	 * @param sinkRecord
	 *            Kafka unit of message
	 * @return Kinesis unit of message
	 */
	public static Record createRecord(SinkRecord sinkRecord) {
		return new Record().withData(parseValue(sinkRecord.valueSchema(), sinkRecord.value()));
	}

}
