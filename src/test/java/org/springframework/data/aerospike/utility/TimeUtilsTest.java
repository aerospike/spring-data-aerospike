package org.springframework.data.aerospike.utility;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.aerospike.SampleClasses.EXPIRATION_ONE_SECOND;

public class TimeUtilsTest {

    @Test
    public void shouldConvertOffsetInSecondsToUnixTime() {
        long nowPlusOneSecond = LocalDateTime.now().plusSeconds(EXPIRATION_ONE_SECOND).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        long actual = TimeUtils.offsetInSecondsToUnixTime(EXPIRATION_ONE_SECOND);

        assertThat(actual).isCloseTo(nowPlusOneSecond, Offset.offset(100L));
    }

    @Test
    public void shouldConvertUnixTimeToOffsetInSeconds() {
        long nowPlusOneSecond = LocalDateTime.now().plusSeconds(EXPIRATION_ONE_SECOND).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        int actual = TimeUtils.unixTimeToOffsetInSeconds(nowPlusOneSecond);

        assertThat(actual).isEqualTo(EXPIRATION_ONE_SECOND);
    }
}
