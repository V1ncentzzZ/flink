package org.apache.flink.connectors.http.table.converter;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Converter that is responsible to convert between http response and Flink SQL internal data structure {@link RowData}.
 */
public class HttpRowConverter implements Serializable {

	protected final RowType rowType;
	protected final HttpDeserializationConverter[] toInternalConverters;
	protected final LogicalType[] fieldTypes;

	public HttpRowConverter(RowType rowType) {
		this.rowType = checkNotNull(rowType);
		this.fieldTypes =
			rowType.getFields().stream()
				.map(RowType.RowField::getType)
				.toArray(LogicalType[]::new);
		this.toInternalConverters = new HttpDeserializationConverter[rowType.getFieldCount()];
		for (int i = 0; i < rowType.getFieldCount(); i++) {
			toInternalConverters[i] = createNullableInternalConverter(rowType.getTypeAt(i));
		}
	}

	/**
	 * Convert data retrieved from http response to internal {@link RowData}.
	 *
	 * @param map result from http request
	 */
	public RowData toInternal(Map map) throws SQLException {
		GenericRowData genericRowData = new GenericRowData(map.size());
		Object[] values = map.values().toArray();
		for (int i = 0; i < values.length; i++) {
			genericRowData.setField(i, toInternalConverters[i].deserialize(values[i]));
		}
		return genericRowData;
	}

	/**
	 * Create a nullable runtime {@link HttpDeserializationConverter} from given {@link
	 * LogicalType}.
	 */
	protected HttpDeserializationConverter createNullableInternalConverter(LogicalType type) {
		return wrapIntoNullableInternalConverter(createInternalConverter(type));
	}

	protected HttpDeserializationConverter wrapIntoNullableInternalConverter(
		HttpDeserializationConverter httpDeserializationConverter) {
		return val -> {
			if (val == null) {
				return null;
			} else {
				return httpDeserializationConverter.deserialize(val);
			}
		};
	}

	protected HttpDeserializationConverter createInternalConverter(LogicalType type) {
		switch (type.getTypeRoot()) {
			case NULL:
				return val -> null;
			case BOOLEAN:
			case FLOAT:
			case DOUBLE:
			case INTERVAL_YEAR_MONTH:
			case INTERVAL_DAY_TIME:
				return val -> val;
			case TINYINT:
				return val -> ((Integer) val).byteValue();
			case SMALLINT:
				// Converter for small type that casts value to int and then return short value,
				return val -> val instanceof Integer ? ((Integer) val).shortValue() : val;
			case INTEGER:
				return val -> val;
			case BIGINT:
				return val -> val;
			case DECIMAL:
				final int precision = ((DecimalType) type).getPrecision();
				final int scale = ((DecimalType) type).getScale();
				// using decimal(20, 0) to support db type bigint unsigned, user should define
				// decimal(20, 0) in SQL,
				// but other precision like decimal(30, 0) can work too from lenient consideration.
				return val ->
					val instanceof BigInteger
						? DecimalData.fromBigDecimal(
						new BigDecimal((BigInteger) val, 0), precision, scale)
						: DecimalData.fromBigDecimal((BigDecimal) val, precision, scale);
			case DATE:
				return val -> (int) (((Date) val).toLocalDate().toEpochDay());
			case TIME_WITHOUT_TIME_ZONE:
				return val -> (int) (((Time) val).toLocalTime().toNanoOfDay() / 1_000_000L);
			case TIMESTAMP_WITH_TIME_ZONE:
			case TIMESTAMP_WITHOUT_TIME_ZONE:
				return val ->
					val instanceof LocalDateTime
						? TimestampData.fromLocalDateTime((LocalDateTime) val)
						: TimestampData.fromTimestamp((Timestamp) val);
			case CHAR:
			case VARCHAR:
				return val -> StringData.fromString((String) val);
			case BINARY:
			case VARBINARY:
				return val -> (byte[]) val;
			case ARRAY:
			case ROW:
			case MAP:
			case MULTISET:
			case RAW:
			default:
				throw new UnsupportedOperationException("Unsupported type:" + type);
		}
	}

	/** Runtime converter to convert http response's field to {@link RowData} type object. */
	@FunctionalInterface
	interface HttpDeserializationConverter extends Serializable {
		/**
		 * Convert a http response's field object to the internal data structure object.
		 *
		 * @param field
		 */
		Object deserialize(Object field) throws SQLException;
	}
}
