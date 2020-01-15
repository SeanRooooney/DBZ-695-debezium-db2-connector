/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2;
import com.ibm.db2.jcc.DB2Driver;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Types;
import java.time.ZoneOffset;
import java.util.Arrays;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.data.SpecialValueDecimal;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;

/**
 * Conversion of DB2 specific datatypes.
 *
 * @author Jiri Pechanec, Peter Urbanetz
 *
 */
public class Db2ValueConverters extends JdbcValueConverters {

    public Db2ValueConverters() {
    }

    /**
     * Create a new instance that always uses UTC for the default time zone when
     * converting values without timezone information to values that require
     * timezones.
     * <p>
     *
     * @param decimalMode
     *            how {@code DECIMAL} and {@code NUMERIC} values should be
     *            treated; may be null if
     *            {@link io.debezium.jdbc.JdbcValueConverters.DecimalMode#PRECISE}
     *            is to be used
     * @param temporalPrecisionMode
     *            date/time value will be represented either as Connect datatypes or Debezium specific datatypes
     */
    public Db2ValueConverters(DecimalMode decimalMode, TemporalPrecisionMode temporalPrecisionMode) {
        super(decimalMode, temporalPrecisionMode, ZoneOffset.UTC, null, null);
    }

    @Override
    public SchemaBuilder schemaBuilder(Column column) {
        switch (column.jdbcType()) {
            // Numeric integers
            case Types.TINYINT:
                // values are an 8-bit unsigned integer value between 0 and 255, we thus need to store it in short int
                return SchemaBuilder.int16();
            case 1111:
            	return SpecialValueDecimal.builder(decimalMode, column.length(), 0);
            /**
            // Floating point
            case microsoft.sql.Types.SMALLMONEY:
            case microsoft.sql.Types.MONEY:
                return SpecialValueDecimal.builder(decimalMode, column.length(), column.scale().get());
            case microsoft.sql.Types.DATETIMEOFFSET:
                return ZonedTimestamp.builder();
             **/
            default:
                return super.schemaBuilder(column);
        }
    }

    @Override
    public ValueConverter converter(Column column, Field fieldDefn) {
        switch (column.jdbcType()) {
            // Numeric integers
            case Types.TINYINT:
                // values are an 8-bit unsigned integer value between 0 and 255, we thus need to store it in short int
                return (data) -> convertSmallInt(column, fieldDefn, data);
            case 1111:
            	//return (data) -> convertDECFloat(column, fieldDefn, data);
            	return (data) -> convertDecimal(column, fieldDefn, data);
             /**
            // Floating point
            case microsoft.sql.Types.SMALLMONEY:
            case microsoft.sql.Types.MONEY:
                return (data) -> convertDecimal(column, fieldDefn, data);
            case microsoft.sql.Types.DATETIMEOFFSET:
                return (data) -> convertTimestampWithZone(column, fieldDefn, data);
            **/
            // TODO Geometry and geography supported since 6.5.0
            default:
                return super.converter(column, fieldDefn);
        }
    }

    /**
     * Time precision in DB2 is defined in scale, the default one is 7
     */
    @Override
    protected int getTimePrecision(Column column) {
        return column.scale().get();
    }

    protected Object convertTimestampWithZone(Column column, Field fieldDefn, Object data) {
        // dummy return
        return super.convertTimestampWithZone(column, fieldDefn, data);
        /**
        if (!(data instanceof DateTimeOffset)) {
            return super.convertTimestampWithZone(column, fieldDefn, data);
        }
        
        final DateTimeOffset dto = (DateTimeOffset) data;
        
        // Timestamp is provided in UTC time
        final Timestamp utc = dto.getTimestamp();
        final ZoneOffset offset = ZoneOffset.ofTotalSeconds(dto.getMinutesOffset() * 60);
        
        return super.convertTimestampWithZone(column, fieldDefn, LocalDateTime.ofEpochSecond(utc.getTime() / 1000, utc.getNanos(), offset).atOffset(offset));
        **/
    }
    
}
    
 