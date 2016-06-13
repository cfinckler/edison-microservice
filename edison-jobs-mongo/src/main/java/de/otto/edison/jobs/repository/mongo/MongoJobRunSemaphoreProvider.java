package de.otto.edison.jobs.repository.mongo;

import com.mongodb.MongoWriteException;
import de.otto.edison.jobs.service.JobRunSemaphoreProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.mongodb.ErrorCategory.DUPLICATE_KEY;


@Service(value = "jobRunSemaphoreProvider")
public class MongoJobRunSemaphoreProvider implements JobRunSemaphoreProvider {

    private final MongoJobRunSemaphoreRepository jobRunSemaphoreRepositoryRepository;

    private Clock clock;

    @Autowired
    public MongoJobRunSemaphoreProvider(MongoJobRunSemaphoreRepository jobRunSemaphoreRepositoryRepository) {
        this(jobRunSemaphoreRepositoryRepository, Clock.systemDefaultZone());

    }

    public MongoJobRunSemaphoreProvider(MongoJobRunSemaphoreRepository jobRunSemaphoreRepositoryRepository, Clock clock) {
        this.jobRunSemaphoreRepositoryRepository = jobRunSemaphoreRepositoryRepository;
        this.clock = clock;
    }

    /**
     * Creates a new running semaphore. If the creation fails with a duplicate key error, it means, the lock has already been acquired.
     * If any other runtime exception occures, it gets thrown and has to be handled outside.
     */
    @Override
    public boolean getRunSemaphoreForJobType(String jobType) {
        try {
            jobRunSemaphoreRepositoryRepository.create(new JobRunSemaphore(jobType, OffsetDateTime.now(clock)));
        }catch (MongoWriteException e) {
            if (e.getError().getCategory().equals(DUPLICATE_KEY)) {
                return false;
            } else {
                throw e;
            }
        }
        return true;
    }


    @Override
    public boolean getRunSemaphoresForJobTypes(Set<String> jobTypes) {
        //Aquire locks always in the same order to avoid getting a deadlock
        List<String> orderedJobTypes = jobTypes.stream().sorted(String.CASE_INSENSITIVE_ORDER).collect(Collectors.toList());

        List<String> obtainedSemaphores = new ArrayList<>();
        try {
            for(String jobType : orderedJobTypes) {
                if (getRunSemaphoreForJobType(jobType)) {
                    obtainedSemaphores.add(jobType);
                } else {
                    break;
                }

            }
        } finally {
            if (obtainedSemaphores.size() != jobTypes.size()) {
                obtainedSemaphores.forEach(this::removeRunSemaphoreForJobType);
            }

        }
        return obtainedSemaphores.size() == jobTypes.size();
    }

    @Override
    public void removeRunSemaphoresForJobTypes(Set<String> jobTypes) {
        //Release locks always in the opposite order as in aquiring
        List<String> orderedJobTypes = jobTypes.stream().sorted(String.CASE_INSENSITIVE_ORDER.reversed()).collect(Collectors.toList());
        orderedJobTypes.forEach(this::removeRunSemaphoreForJobType);
    }

    @Override
    public void removeRunSemaphoreForJobType(String jobType) {
        jobRunSemaphoreRepositoryRepository.delete(jobType);
    }
}
