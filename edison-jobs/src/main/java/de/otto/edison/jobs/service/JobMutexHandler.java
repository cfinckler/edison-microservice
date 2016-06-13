package de.otto.edison.jobs.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.HashSet;
import java.util.Set;

import static java.util.Collections.emptySet;

@Service
public class JobMutexHandler {

    @Autowired(required = false)
    private Set<JobMutexGroup> mutexGroups;

    @Autowired
    private JobRunSemaphoreProvider jobRunSemaphoreProvider;

    public JobMutexHandler() {
    }

    public JobMutexHandler(Set<JobMutexGroup> mutexGroups, JobRunSemaphoreProvider jobRunSemaphoreProvider) {
        this.mutexGroups = mutexGroups;
        this.jobRunSemaphoreProvider = jobRunSemaphoreProvider;
    }

    @PostConstruct
    private void postConstruct() {
        if (mutexGroups == null) {
            this.mutexGroups = emptySet();
        }
    }

    public boolean isJobStartable(String jobType) {
        final Set<String> mutexJobTypes = mutexJobTypesFor(jobType);
        return jobRunSemaphoreProvider.getRunSemaphoresForJobTypes(mutexJobTypes);
    }

    public void jobHasStopped(String jobType) {
        final Set<String> mutexJobTypes = mutexJobTypesFor(jobType);
        jobRunSemaphoreProvider.removeRunSemaphoresForJobTypes(mutexJobTypes);
    }

    private Set<String> mutexJobTypesFor(final String jobType) {
        final Set<String> result = new HashSet<>();
        result.add(jobType);
        this.mutexGroups
                .stream()
                .map(JobMutexGroup::getJobTypes)
                .filter(g->g.contains(jobType))
                .forEach(result::addAll);
        return result;
    }
}
