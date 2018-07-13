package org.springframework.data.aerospike;

import java.util.concurrent.*;

import static java.util.Collections.nCopies;
import static java.util.concurrent.CompletableFuture.runAsync;

public class AsyncUtils {

	public static void executeConcurrently(int numThreads, Runnable task) throws Exception {
		ExecutorService pool = Executors.newFixedThreadPool(numThreads);

		try {
			CountDownLatch countDownLatch = new CountDownLatch(1);
			CompletableFuture future = nCopies(numThreads, withCountDownLatch(task, countDownLatch))
					.stream()
					.map(runnable -> runAsync(runnable, pool))
					.reduce((l, r) -> CompletableFuture.allOf(l, r))
					.get();

			countDownLatch.countDown();
			future.get(5, TimeUnit.SECONDS);
		} finally {
			pool.shutdownNow();
		}

	}

	private static Runnable withCountDownLatch(Runnable task, CountDownLatch countDownLatch) {
		return () -> {
			try {
				countDownLatch.await();
				task.run();
			} catch (InterruptedException e) {
				throw new RuntimeException("Thread is interrupted", e);
			}
		};
	}
}
