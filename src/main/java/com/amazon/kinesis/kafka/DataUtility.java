package com.amazon.kinesis.kafka;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.HashMap;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
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
        if (value instanceof Double) {
            ByteBuffer buf = ByteBuffer.allocate(8);
            buf.putDouble((Double) value);
            return buf;
        }
        if (value instanceof Float) {
            ByteBuffer buf = ByteBuffer.allocate(4);
            buf.putFloat((Float) value);
            return buf;
        }
        if (value instanceof Short) {
            ByteBuffer buf = ByteBuffer.allocate(2);
            buf.putShort((Short) value);
            return buf;
        }
        if (value instanceof Integer) {
            ByteBuffer buf = ByteBuffer.allocate(4);
            buf.putInt((Integer) value);
            return buf;
        }
        if (value instanceof Byte) {
            ByteBuffer buf = ByteBuffer.allocate(1);
            buf.put((Byte) value);
            return buf;
        }
            if (value instanceof Long) {
                ByteBuffer buf = ByteBuffer.allocate(8);
                buf.putLong((Long) value);
                return buf;
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
                    return parseValue(null, json);
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

        if (value.getClass().isArray()){

            Object[] objs = (Object[]) value;
            ByteBuffer[] byteBuffers = new ByteBuffer[objs.length];
            int noOfByteBuffer = 0;

            for (Object obj : objs) {
                byteBuffers[noOfByteBuffer++] = parseValue(null, obj);
            }

            ByteBuffer result = ByteBuffer.allocate(Arrays.stream(byteBuffers).mapToInt(Buffer::remaining).sum());
            Arrays.stream(byteBuffers).forEach(bb -> result.put(bb.duplicate()));
            return result;
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
