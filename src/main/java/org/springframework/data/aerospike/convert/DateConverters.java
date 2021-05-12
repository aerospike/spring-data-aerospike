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

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Out of the box conversions for java dates and calendars.
 *
 * @author Michael Nitschinger
 */
public final class DateConverters {

  private DateConverters() {
  }

  /**
   * Returns all converters by this class that can be registered.
   *
   * @return the list of converters to register.
   */
  public static Collection<Converter<?, ?>> getConvertersToRegister() {
    List<Converter<?, ?>> converters = new ArrayList<>();

    converters.add(Java8LocalDateTimeToLongConverter.INSTANCE);
    converters.add(NumberToJava8LocalDateTimeConverter.INSTANCE);

    return converters;
  }

  @WritingConverter
  public enum Java8LocalDateTimeToLongConverter implements Converter<LocalDateTime, Long> {
    INSTANCE;

    @Override
    public Long convert(LocalDateTime source) {
      return source == null ? null : source.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }
  }

  @ReadingConverter
  public enum NumberToJava8LocalDateTimeConverter implements Converter<Number, LocalDateTime> {
    INSTANCE;

    @Override
    public LocalDateTime convert(Number source) {
      if (source == null) {
        return null;
      }

      return LocalDateTime.ofInstant(Instant.ofEpochMilli(source.longValue()), ZoneId.systemDefault());
    }
  }
}
