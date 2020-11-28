package org.apache.flink.formats.protobuf;


import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.joda.time.DateTime;
import org.joda.time.DateTimeFieldType;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.MapEntry;
import com.google.protobuf.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProtobufRowDeserializationSchema implements DeserializationSchema<Row> {
	private static final Logger LOG = LoggerFactory.getLogger(ProtobufRowDeserializationSchema.class);
    /**
     * Used for time conversions into SQL types.
     */
    private static final TimeZone LOCAL_TZ = TimeZone.getDefault();

    /**
     * Protobuf message class for deserialization. Might be null if message class is not available.
     */
    private Class<? extends Message> messageClazz;

    /**
     * Type information describing the result type.
     */
    private RowTypeInfo typeInfo;

	/**
	 * all field from ddl
	 */
	private List<String> fieldNames;

	/** Flag indicating whether to ignore invalid fields/rows (default: throw an exception). */
	private boolean ignoreParseErrors;

	private transient Message defaultInstance;

	private transient DeserializationRuntimeConverter deserializationRuntimeConverter;

	@FunctionalInterface
    interface DeserializationRuntimeConverter extends Serializable {
        Object convert(Object object);
    }

    /**
     * Creates a Protobuf deserialization descriptor for the given message class. Having the
     * concrete Protobuf message class might improve performance.
     *
     * @param messageClazz Protobuf message class used to deserialize Protobuf's message to Flink's row
     */
    public ProtobufRowDeserializationSchema(Class<? extends GeneratedMessageV3> messageClazz,
											TypeInformation<Row> typeInfo,
											TableSchema tableSchema,
											boolean ignoreParseErrors) {
        Preconditions.checkNotNull(messageClazz, "Protobuf message class must not be null.");
        this.messageClazz = messageClazz;
		this.typeInfo = (RowTypeInfo) typeInfo;
		// TODO support nested field missing
		this.fieldNames = Arrays.asList(tableSchema.getFieldNames());
		LOG.debug("all field names: {}", fieldNames);
		this.ignoreParseErrors = ignoreParseErrors;
		this.defaultInstance = ProtobufUtils.getDefaultInstance(messageClazz);

		Descriptors.Descriptor descriptor = ProtobufUtils.getDescriptor(this.messageClazz);
		List<FieldDescriptor> fieldDescriptors = compareSchemas(descriptor.getFields());
		this.deserializationRuntimeConverter = this.createRowConverter(fieldDescriptors, this.typeInfo);
	}

    @Override
    public Row deserialize(byte[] bytes) throws IOException {
        try {
			Message message = this.defaultInstance
                    .newBuilderForType()
                    .mergeFrom(bytes)
                    .build();
			return (Row) this.deserializationRuntimeConverter.convert(message);
        } catch (Exception e) {
        	String msg = "Failed to deserialize Protobuf message.";
        	if (ignoreParseErrors) {
        		LOG.error(msg, e);
        		return null;
			} else {
				throw new IOException(msg, e);
			}
        }
    }

	@Override
    public TypeInformation<Row> getProducedType() {
        return this.typeInfo;
    }

	@Override
	public boolean isEndOfStream(Row nextElement) {
		return false;
	}

    // --------------------------------------------------------------------------------------------

    private DeserializationRuntimeConverter createRowConverter(List<FieldDescriptor> fieldDescriptors, RowTypeInfo rowTypeInfo) {
        final TypeInformation<?>[] fieldTypeInfos = rowTypeInfo.getFieldTypes();
        final int length = fieldDescriptors.size();

        final DeserializationRuntimeConverter[] deserializationRuntimeConverters = new DeserializationRuntimeConverter[length];

        for (int i = 0; i < length; i++) {
            final FieldDescriptor fieldDescriptor = fieldDescriptors.get(i);
			final TypeInformation<?> fieldTypeInfo = fieldTypeInfos[i];
			deserializationRuntimeConverters[i] = createConverter(fieldDescriptor, fieldTypeInfo);
        }

        return (Object o) -> {
            Message message = (Message) o;
            final Row row = new Row(length);
            for (int i = 0; i < length; i++) {
				FieldDescriptor fieldDescriptor = fieldDescriptors.get(i);
				Object fieldO = message.getField(fieldDescriptor);
                row.setField(i, deserializationRuntimeConverters[i].convert(fieldO));
            }
            return row;
        };
    }

    @SuppressWarnings("unchecked")
    private DeserializationRuntimeConverter createConverter(Descriptors.GenericDescriptor genericDescriptor, TypeInformation<?> info) {
        // we perform the conversion based on descriptor information but enriched with pre-computed
        // type information where useful (i.e., for list)

        if (genericDescriptor instanceof Descriptors.Descriptor) {
			List<FieldDescriptor> fields = ((Descriptors.Descriptor) genericDescriptor).getFields();
			return createRowConverter(fields, (RowTypeInfo) info);

        } else if (genericDescriptor instanceof FieldDescriptor) {

            FieldDescriptor fieldDescriptor = ((FieldDescriptor) genericDescriptor);

            // field
            switch (fieldDescriptor.getType()) {
                case INT32:
                case FIXED32:
                case UINT32:
                case SFIXED32:
                case SINT32:
                case INT64:
                case UINT64:
                case FIXED64:
                case SFIXED64:
                case SINT64:
                case DOUBLE:
                case FLOAT:
                case BOOL:
                case STRING:
                    if (info instanceof ObjectArrayTypeInfo) {
                        // list
                        TypeInformation<?> elementTypeInfo = ((ObjectArrayTypeInfo) info).getComponentInfo();

                        return this.createArrayConverter(elementTypeInfo);
                    } else {
                        return this.createObjectConverter(info);
                    }
                case ENUM:
                    if (info instanceof ObjectArrayTypeInfo) {
                        // list
                        return (Object o) -> ((List) o)
                                .stream()
                                .map(e -> ((Descriptors.EnumValueDescriptor) e).getIndex())
								.toArray();
                    } else {
                        return (Object o) -> ((Descriptors.EnumValueDescriptor) o).getIndex();
                    }
                case GROUP:
                case MESSAGE:
                    if (info instanceof ObjectArrayTypeInfo) {
                        // list
                        TypeInformation<?> elementTypeInfo = ((ObjectArrayTypeInfo) info).getComponentInfo();
                        Descriptors.Descriptor elementDescriptor = fieldDescriptor.getMessageType();
                        DeserializationRuntimeConverter elementConverter = this.createConverter(elementDescriptor, elementTypeInfo);

                        return (Object o) -> ((List) o)
                                .stream()
                                .map(elementConverter::convert)
                                .toArray();

                    } else if (info instanceof MapTypeInfo) {
                        // map
                        final MapTypeInfo<?, ?> mapTypeInfo = (MapTypeInfo<?, ?>) info;

                        boolean isDynamicMessage = false;

                        if (this.messageClazz == null) {
                            isDynamicMessage = true;
                        }

                        // todo map's key only support string
                        final DeserializationRuntimeConverter keyConverter = Object::toString;

                        final FieldDescriptor keyFieldDescriptor =
                                fieldDescriptor.getMessageType().getFields().get(0);

                        final FieldDescriptor valueFieldDescriptor =
                                fieldDescriptor.getMessageType().getFields().get(1);

                        final TypeInformation<?> valueTypeInfo =
                                mapTypeInfo.getValueTypeInfo();

                        final DeserializationRuntimeConverter valueConverter =
                                createConverter(valueFieldDescriptor, valueTypeInfo);

                        if (isDynamicMessage) {

                            return (Object o) -> {
                                final List<DynamicMessage> dynamicMessages = (List<DynamicMessage>) o;

                                final Map<String, Object> convertedMap = new HashMap<>(dynamicMessages.size());

                                dynamicMessages.forEach((DynamicMessage dynamicMessage) -> {
                                    convertedMap.put(
                                            (String) keyConverter.convert(dynamicMessage.getField(keyFieldDescriptor))
                                            , valueConverter.convert(dynamicMessage.getField(valueFieldDescriptor)));
                                });

                                return convertedMap;
                            };

                        } else {

                            return (Object o) -> {
                                final List<MapEntry> mapEntryList = (List<MapEntry>) o;
                                final Map<String, Object> convertedMap = new HashMap<>(mapEntryList.size());
                                mapEntryList.forEach((MapEntry message) -> {
                                    convertedMap.put(
                                            (String) keyConverter.convert(message.getKey())
                                            , valueConverter.convert(message.getValue()));
                                });

                                return convertedMap;
                            };
                        }
                    } else if (info instanceof RowTypeInfo) {
                        // row
                        return createRowConverter(((FieldDescriptor) genericDescriptor).getMessageType().getFields(), (RowTypeInfo) info);
                    }
                    throw new IllegalStateException("Message expected but was: " + fieldDescriptor);
                case BYTES:
					if (info instanceof ObjectArrayTypeInfo) {

						return (Object o) -> ((List) o)
							.stream()
							.map(e -> {
								byte[] bytes = ((ByteString) e).toByteArray();
								if (Types.BIG_DEC == info) {
									return convertToDecimal(bytes);
								}
								return bytes;
							})
							.toArray();
					} else {
						return (Object o) -> {
							final byte[] bytes = ((ByteString) o).toByteArray();
							if (Types.BIG_DEC == info) {
								return convertToDecimal(bytes);
							}
							return bytes;
						};
					}
            }
        }

        throw new IllegalArgumentException("Unsupported Protobuf type '" + genericDescriptor.getName() + "'.");

    }

    @SuppressWarnings("unchecked")
    private DeserializationRuntimeConverter createArrayConverter(TypeInformation<?> info) {

        DeserializationRuntimeConverter elementConverter;

        if (Types.SQL_DATE == info) {

            elementConverter = this::convertToDate;

        } else if (Types.SQL_TIME == info) {

            elementConverter = this::convertToTime;
        } else {

            elementConverter = (Object fieldO) -> (fieldO);
        }

        return (Object o) -> ((List) o)
                .stream()
                .map(elementConverter::convert)
				.toArray();
    }

    private DeserializationRuntimeConverter createObjectConverter(TypeInformation<?> info) {
        if (Types.SQL_DATE == info) {
            return this::convertToDate;
        } else if (Types.SQL_TIME == info) {
            return this::convertToTime;
        } else {
            return (Object o) -> o;
        }
    }

    // --------------------------------------------------------------------------------------------

    private BigDecimal convertToDecimal(byte[] bytes) {
        return new BigDecimal(new BigInteger(bytes));
    }

    private Date convertToDate(Object object) {
        final long millis;
        if (object instanceof Integer) {
            final Integer value = (Integer) object;
            // adopted from Apache Calcite
            final long t = (long) value * 86400000L;
            millis = t - (long) LOCAL_TZ.getOffset(t);
        } else {
            // use 'provided' Joda time
            final LocalDate value = (LocalDate) object;
            millis = value.toDate().getTime();
        }
        return new Date(millis);
    }

    private Time convertToTime(Object object) {
        final long millis;
        if (object instanceof Integer) {
            millis = (Integer) object;
        } else {
            // use 'provided' Joda time
            final LocalTime value = (LocalTime) object;
            millis = value.get(DateTimeFieldType.millisOfDay());
        }
        return new Time(millis - LOCAL_TZ.getOffset(millis));
    }

    private Timestamp convertToTimestamp(Object object) {
        final long millis;
        if (object instanceof Long) {
            millis = (Long) object;
        } else {
            // use 'provided' Joda time
            final DateTime value = (DateTime) object;
            millis = value.toDate().getTime();
        }
        return new Timestamp(millis - LOCAL_TZ.getOffset(millis));
    }

	private void writeObject(ObjectOutputStream outputStream) throws IOException {
    	Map<String, Object> map = new HashMap<>();
    	map.put("messageClazz", this.messageClazz);
    	map.put("typeInfo", this.typeInfo);
    	map.put("fieldNames", this.fieldNames);
    	map.put("ignoreParseErrors", this.ignoreParseErrors);
		outputStream.writeObject(map);
    }

    @SuppressWarnings("unchecked")
    private void readObject(ObjectInputStream inputStream) throws ClassNotFoundException, IOException {

        Object o = inputStream.readObject();

        Map<String, Object> map = (Map<String, Object>) o;
		this.messageClazz = (Class<? extends Message>) map.get("messageClazz");
		this.typeInfo = (RowTypeInfo) map.get("typeInfo");
		this.defaultInstance = ProtobufUtils.getDefaultInstance(this.messageClazz);
		this.fieldNames = (List<String>) map.get("fieldNames");
		this.ignoreParseErrors = (boolean) map.get("ignoreParseErrors");

		List<FieldDescriptor> fieldDescriptors = compareSchemas(ProtobufUtils.getDescriptor(this.messageClazz).getFields());
		this.deserializationRuntimeConverter = this.createRowConverter(fieldDescriptors, this.typeInfo);
    }


    private List<FieldDescriptor> compareSchemas(List<FieldDescriptor> schemaFromProtobuf) {
    	List<FieldDescriptor> result = new ArrayList<>();
		outer:for (int i = 0; i < fieldNames.size(); i++) {
			String fieldName = fieldNames.get(i);
			for (int j = 0; j < schemaFromProtobuf.size(); j++) {
				FieldDescriptor fieldDescriptor = schemaFromProtobuf.get(j);
				if (fieldName.equalsIgnoreCase(fieldDescriptor.getName())) {
					result.add(fieldDescriptor);
					continue outer;
				}
			}
			//String msg = String.format("not found field: %s in protobuf", fieldName);
			//throw new FlinkRuntimeException(msg);
		}
		LOG.debug("fields name is: {}, field descriptor is: {}", fieldNames, result);
		return result;
	}
}
