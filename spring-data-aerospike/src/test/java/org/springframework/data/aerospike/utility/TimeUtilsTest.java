package org.springframework.data.aerospike.utility;

import org.assertj.core.data.Offset;
import org.joda.time.DateTime;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.aerospike.SampleClasses.EXPIRATION_ONE_SECOND;

public class TimeUtilsTest {

    @Test
    public void shouldConvertOffsetInSecondsToUnixTime() {
        long expected = DateTime.now().plusSeconds(EXPIRATION_ONE_SECOND).getMillis();
        long actual = TimeUtils.offsetInSecondsToUnixTime(EXPIRATION_ONE_SECOND);

        assertThat(actual).isCloseTo(expected, Offset.offset(100L));
    }

    @Test
    public void shouldConvertUnixTimeToOffsetInSeconds() {
        long nowPlusOneSecond = DateTime.now().plusSeconds(EXPIRATION_ONE_SECOND).getMillis();
        int actual = TimeUtils.unixTimeToOffsetInSeconds(nowPlusOneSecond);

        assertThat(actual).isEqualTo(EXPIRATION_ONE_SECOND);
    }
}
