package org.springframework.data.aerospike;

import lombok.SneakyThrows;

import java.util.concurrent.*;

import static java.util.Collections.nCopies;
import static java.util.concurrent.CompletableFuture.runAsync;

public class AsyncUtils {

	@SneakyThrows
	public static void executeConcurrently(int numThreads, Runnable task) {
		ExecutorService pool = Executors.newFixedThreadPool(numThreads);

		try {
			CountDownLatch countDownLatch = new CountDownLatch(1);
			CompletableFuture<Void> future = nCopies(numThreads, withCountDownLatch(task, countDownLatch))
					.stream()
					.map(runnable -> runAsync(runnable, pool))
					.reduce(CompletableFuture::allOf)
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
