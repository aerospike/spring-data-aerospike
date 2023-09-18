package org.springframework.data.aerospike.query.cache;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;

import static org.springframework.data.aerospike.query.cache.IndexRefresher.CACHE_REFRESH_FREQUENCY_MILLIS;

@Slf4j
@AllArgsConstructor
public class ScheduledIndexRefresher {

    IndexRefresher indexRefresher;

    @Scheduled(fixedRateString = "${" + CACHE_REFRESH_FREQUENCY_MILLIS + "}")
    public void scheduledRefresh() {
        log.debug("Refreshing indexes cache");
        indexRefresher.refreshIndexes();
    }

}
