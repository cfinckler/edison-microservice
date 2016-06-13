package de.otto.edison.jobs.service;

import java.util.Set;

public interface JobRunSemaphoreProvider {

    boolean getRunSemaphoreForJobType(String jobType);

    boolean getRunSemaphoresForJobTypes(Set<String> jobTypes);

    void removeRunSemaphoreForJobType(String jobType);

    void removeRunSemaphoresForJobTypes(Set<String> mutexJobTypes);
}
