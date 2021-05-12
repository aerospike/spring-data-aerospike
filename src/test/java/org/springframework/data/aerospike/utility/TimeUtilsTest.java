package org.springframework.data.aerospike.utility;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.aerospike.SampleClasses.EXPIRATION_ONE_SECOND;

public class TimeUtilsTest {

    @Test
    public void shouldConvertOffsetInSecondsToUnixTime() {
        int expected = LocalDateTime.now().plusSeconds(EXPIRATION_ONE_SECOND).getNano();
        long actual = TimeUtils.offsetInSecondsToUnixTime(EXPIRATION_ONE_SECOND);

        assertThat(actual).isCloseTo(expected, Offset.offset(100L));
    }

    @Test
    public void shouldConvertUnixTimeToOffsetInSeconds() {
        int nowPlusOneSecond = LocalDateTime.now().plusSeconds(EXPIRATION_ONE_SECOND).getNano();
        int actual = TimeUtils.unixTimeToOffsetInSeconds(nowPlusOneSecond);

        assertThat(actual).isEqualTo(EXPIRATION_ONE_SECOND);
    }
}
