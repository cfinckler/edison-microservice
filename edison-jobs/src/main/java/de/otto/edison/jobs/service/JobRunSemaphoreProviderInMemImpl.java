package de.otto.edison.jobs.service;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

public class JobRunSemaphoreProviderInMemImpl implements JobRunSemaphoreProvider {

    private Set<String> semaphores = new HashSet<>();

    @Override
    public synchronized boolean getRunSemaphoreForJobType(String jobType) {
        if (semaphores.contains(jobType)) {
            return false;
        }
        semaphores.add(jobType);
        return true;
    }

    @Override
    public synchronized boolean getRunSemaphoresForJobTypes(Set<String> jobTypes) {
        if (Collections.disjoint(semaphores, jobTypes)) {
            semaphores.addAll(jobTypes);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public synchronized void removeRunSemaphoreForJobType(String jobType) {
        semaphores.remove(jobType);
    }

    @Override
    public synchronized void removeRunSemaphoresForJobTypes(Set<String> jobTypes) {
        semaphores.removeAll(jobTypes);
    }
}
