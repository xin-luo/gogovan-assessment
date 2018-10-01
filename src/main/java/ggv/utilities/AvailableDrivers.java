package ggv.utilities;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * This class keeps a record of distinct drivers who are available for new work. This way, no driver ever gets assigned
 * a job while they're currently on another job
 */
@Slf4j
@Service
public class AvailableDrivers {
    private final BlockingQueue<String> availableDrivers;

    public AvailableDrivers(ConfigurationProvider configuration) {
        availableDrivers = new LinkedBlockingQueue<>();
        final String[] drivers = configuration.getDrivers();

        List<String> initialDrivers = new ArrayList<>();
        for (int i = 0; i < configuration.getNumDrivers(); i++) {
            for (String driver : drivers) {
                initialDrivers.add(driver + i);
            }
        }
        Collections.shuffle(initialDrivers);
        availableDrivers.addAll(initialDrivers);
    }

    public long getNumDrivers() {
        return availableDrivers.size();
    }

    /**
     * Forcibly takes a driver, waiting as long as needed
     */
    public String getDriver() throws InterruptedException {
        return availableDrivers.take();
    }

    /**
     * Waits up to x seconds to get a driver from the pool, returns null if unable to
     */
    public String pollDriver(int secondsToWait) {
        try {
            return availableDrivers.poll(secondsToWait, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.warn("Interrupted while waiting for a driver.");
            return null;
        }
    }

    /**
     * Add a driver back into the pool
     */
    public void addDriver(String driver) {
        availableDrivers.offer(driver);
    }
}