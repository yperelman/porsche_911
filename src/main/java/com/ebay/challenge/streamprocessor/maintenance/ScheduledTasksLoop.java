package com.ebay.challenge.streamprocessor.maintenance;

import com.ebay.challenge.streamprocessor.offset.OffsetCommitTracker;
import com.ebay.challenge.streamprocessor.output.SinkHealthProbe;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Component
public class ScheduledTasksLoop implements SmartLifecycle {

    private final OffsetCommitTracker offsetCommitTracker;
    private final SinkHealthProbe sinkHealthProbe;
    private final long intervalMs;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile Thread worker;

    public ScheduledTasksLoop(
            OffsetCommitTracker offsetCommitTracker,
            SinkHealthProbe sinkHealthProbe,
            @Value("${maintenance.loop.interval-ms:2000}") long intervalMs) {
        if (intervalMs <= 0) {
            throw new IllegalArgumentException("maintenance.loop.interval-ms must be positive");
        }
        this.offsetCommitTracker = offsetCommitTracker;
        this.sinkHealthProbe = sinkHealthProbe;
        this.intervalMs = intervalMs;
    }

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        worker = new Thread(this::runLoop, "stream-maintenance-loop");
        worker.setDaemon(true);
        worker.start();
    }

    private void runLoop() {
        // I use a dedicated loop instead of @Scheduled because Spring's default
        // scheduler is single-threaded unless explicitly configured. If one scheduled
        // task blocks or runs long, other scheduled tasks wait behind it. The annotation
        // also makes this easy to copy without noticing that caveat, which can create
        // unexpected and hard-to-debug production behavior.
        while (running.get()) {
            runSafely("offset commit drain", offsetCommitTracker::commitReadyOffsets);
            runSafely("sink health probe", sinkHealthProbe::probeIfPaused);

            try {
                Thread.sleep(intervalMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                running.set(false);
            }
        }
    }

    private void runSafely(String taskName, Runnable task) {
        try {
            task.run();
        } catch (Exception e) {
            log.warn("maintenance task failed: {}", taskName, e);
        }
    }

    @Override
    public void stop() {
        running.set(false);
        Thread thread = worker;
        if (thread != null) {
            thread.interrupt();
            try {
                thread.join(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void stop(Runnable callback) {
        stop();
        callback.run();
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }
}
