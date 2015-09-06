package org.mogware.messagebus.channels.rabbitmq;

import java.net.URI;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import org.mogware.system.Guid;

public final class ExtensionMethods {
    private ExtensionMethods() {
    }

    public static String toNull(Guid value) {
        return value == Guid.empty ? null : value.toString();
    }

    public static String toEmpty(Guid value) {
        return value == Guid.empty ? "" : value.toString();
    }

    public static Class getType(String type) {
        try {
            return Class.forName(type);
        } catch (ClassNotFoundException ex) {
            return null;
        }
    }

    public static PublicationAddress toExchangeAddress(String exchangeName) {
        return toExchangeAddress(exchangeName, "fanout");
    }

    public static PublicationAddress toExchangeAddress(String exchangeName,
            String exchangeType) {
        if (exchangeName == null || exchangeName.isEmpty())
            return null;
        return new PublicationAddress(exchangeType, exchangeName, "");
    }

    public static Guid toGuid(String value) {
        return Guid.valueOf(value != null ? value : "");
    }

    public static URI toUri(String value) {
        return URI.create(value != null ? value : "");
    }

    public static LocalDateTime toDateTime(Date dt) {
        return LocalDateTime.ofInstant(dt.toInstant(), ZoneId.systemDefault());
    }

    public static LocalDateTime toDateTime(String value, LocalDateTime ldt) {
        try {
            return LocalDateTime.parse(value);
        } catch (DateTimeParseException ex) {
            return ldt.plus(Long.decode(value), ChronoUnit.MILLIS);
        }
    }

    public static String normalizeName(String value) {
        return value == null ? "" : value.toLowerCase().replace('.', '-');
    }
}
