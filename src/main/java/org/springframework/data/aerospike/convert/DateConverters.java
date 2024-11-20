/*
 * Copyright 2012-2015 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.convert;

import org.joda.time.DateMidnight;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.util.ClassUtils;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * Out of the box conversions for java dates and calendars.
 *
 * @author Michael Nitschinger
 */
public final class DateConverters {

    private static final String MILLIS = "millis";
    private static final String TIMEZONE = "timezone";

    private static final boolean JODA_TIME_IS_PRESENT = ClassUtils.isPresent("org.joda.time.LocalDate", null);

    private DateConverters() {
    }

    /**
     * Returns all converters by this class that can be registered.
     *
     * @return the list of converters to register.
     */
    public static Collection<Converter<?, ?>> getConvertersToRegister() {
        List<Converter<?, ?>> converters = new ArrayList<>();

        converters.add(DateToLongConverter.INSTANCE);
        converters.add(LongToDateConverter.INSTANCE);
        converters.add(CalendarToMapConverter.INSTANCE);
        converters.add(MapToCalendarConverter.INSTANCE);
        converters.add(Java8LocalDateTimeToLongConverter.INSTANCE);
        converters.add(Java8LocalDateToLongConverter.INSTANCE);
        converters.add(LongToJava8LocalDateTimeConverter.INSTANCE);
        converters.add(LongToJava8LocalDateConverter.INSTANCE);
        converters.add(DurationToStringConverter.INSTANCE);
        converters.add(StringToDurationConverter.INSTANCE);
        converters.add(InstantToLongConverter.INSTANCE);
        converters.add(LongToInstantConverter.INSTANCE);

        if (JODA_TIME_IS_PRESENT) {
            converters.add(LocalDateToLongConverter.INSTANCE);
            converters.add(LongToLocalDateConverter.INSTANCE);
            converters.add(LocalDateTimeToLongConverter.INSTANCE);
            converters.add(LongToLocalDateTimeConverter.INSTANCE);
            converters.add(DateTimeToLongConverter.INSTANCE);
            converters.add(LongToDateTimeConverter.INSTANCE);
            converters.add(DateMidnightToLongConverter.INSTANCE);
            converters.add(LongToDateMidnightConverter.INSTANCE);
        }

        return converters;
    }

    @WritingConverter
    public enum DateToLongConverter implements Converter<Date, Long> {
        INSTANCE;

        @Override
        public Long convert(Date source) {
            return source == null ? null : source.getTime();
        }
    }

    @ReadingConverter
    public enum LongToDateConverter implements Converter<Long, Date> {
        INSTANCE;

        @Override
        public Date convert(Long source) {
            if (source == null) {
                return null;
            }

            Date date = new Date();
            date.setTime(source);
            return date;
        }
    }

    @WritingConverter
    public enum CalendarToMapConverter implements Converter<Calendar, Map<String, String>> {
        INSTANCE;

        @Override
        public Map<String, String> convert(Calendar source) {
            if (source == null) return null;

            Map<String, String> map = new HashMap<>();
            map.put(MILLIS, String.valueOf(source.getTimeInMillis()));
            map.put(TIMEZONE, source.getTimeZone().getID());
            return map;
        }
    }

    @ReadingConverter
    public enum MapToCalendarConverter implements Converter<Map<String, String>, Calendar> {
        INSTANCE;

        @Override
        public Calendar convert(Map<String, String> source) {
            if (source == null) {
                return null;
            }

            Calendar calendar = Calendar.getInstance();
            calendar.setTimeZone(TimeZone.getTimeZone(source.get(TIMEZONE)));
            calendar.setTimeInMillis(Long.parseLong(source.get(MILLIS)));
            return calendar;
        }
    }

    @WritingConverter
    public enum Java8LocalDateTimeToLongConverter implements Converter<java.time.LocalDateTime, Long> {
        INSTANCE;

        @Override
        public Long convert(java.time.LocalDateTime source) {
            return source == null ? null : source.atZone(ZoneOffset.UTC).toInstant().toEpochMilli();
        }
    }

    @ReadingConverter
    public enum LongToJava8LocalDateTimeConverter implements Converter<Long, java.time.LocalDateTime> {
        INSTANCE;

        @Override
        public java.time.LocalDateTime convert(Long source) {
            if (source == null) {
                return null;
            }

            return java.time.LocalDateTime.ofInstant(Instant.ofEpochMilli(source), ZoneOffset.UTC);
        }
    }

    @WritingConverter
    public enum Java8LocalDateToLongConverter implements Converter<java.time.LocalDate, Long> {
        INSTANCE;

        @Override
        public Long convert(java.time.LocalDate source) {
            return source == null ? null : source.toEpochDay();
        }
    }

    @ReadingConverter
    public enum LongToJava8LocalDateConverter implements Converter<Long, java.time.LocalDate> {
        INSTANCE;

        @Override
        public java.time.LocalDate convert(Long source) {
            if (source == null) {
                return null;
            }

            return java.time.LocalDate.ofEpochDay(source);
        }
    }

    @WritingConverter
    public enum LocalDateToLongConverter implements Converter<LocalDate, Long> {
        INSTANCE;

        @Override
        public Long convert(LocalDate source) {
            return source == null ? null : source.toDate().getTime();
        }
    }

    @ReadingConverter
    public enum LongToLocalDateConverter implements Converter<Long, LocalDate> {
        INSTANCE;

        @Override
        public LocalDate convert(Long source) {
            return source == null ? null : new LocalDate(source);
        }
    }

    @WritingConverter
    public enum LocalDateTimeToLongConverter implements Converter<LocalDateTime, Long> {
        INSTANCE;

        @Override
        public Long convert(LocalDateTime source) {
            return source == null ? null : source.toDate().getTime();
        }
    }

    @ReadingConverter
    public enum LongToLocalDateTimeConverter implements Converter<Long, LocalDateTime> {
        INSTANCE;

        @Override
        public LocalDateTime convert(Long source) {
            return source == null ? null : new LocalDateTime(source);
        }
    }

    @WritingConverter
    public enum DateTimeToLongConverter implements Converter<DateTime, Long> {
        INSTANCE;

        @Override
        public Long convert(DateTime source) {
            return source == null ? null : source.toDate().getTime();
        }
    }

    @ReadingConverter
    public enum LongToDateTimeConverter implements Converter<Long, DateTime> {
        INSTANCE;

        @Override
        public DateTime convert(Long source) {
            return source == null ? null : new DateTime(source);
        }
    }

    @WritingConverter
    public enum DateMidnightToLongConverter implements Converter<DateMidnight, Long> {
        INSTANCE;

        @Override
        public Long convert(DateMidnight source) {
            return source == null ? null : source.toDate().getTime();
        }
    }

    @ReadingConverter
    public enum LongToDateMidnightConverter implements Converter<Long, DateMidnight> {
        INSTANCE;

        @Override
        public DateMidnight convert(Long source) {
            return source == null ? null : new DateMidnight(source);
        }
    }

    @WritingConverter
    public enum DurationToStringConverter implements Converter<Duration, String> {
        INSTANCE;

        @Override
        public String convert(Duration source) {
            if (source == null) return null;

            return source.toString();
        }
    }

    @ReadingConverter
    public enum StringToDurationConverter implements Converter<String, Duration> {
        INSTANCE;

        @Override
        public Duration convert(String source) {
            if (source == null) {
                return null;
            }

            return Duration.parse(source);
        }
    }

    @WritingConverter
    public enum InstantToLongConverter implements Converter<Instant, Long> {
        INSTANCE;

        @Override
        public Long convert(Instant source) {
            return source == null ? null : source.toEpochMilli();
        }
    }

    @ReadingConverter
    public enum LongToInstantConverter implements Converter<Long, Instant> {
        INSTANCE;

        @Override
        public Instant convert(Long source) {
            return source == null ? null : Instant.ofEpochMilli(source);
        }
    }
}
