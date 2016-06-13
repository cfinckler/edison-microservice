package de.otto.edison.jobs.repository.mongo;

import com.mongodb.MongoWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.WriteError;
import de.otto.edison.testsupport.util.TestClock;
import org.bson.BsonDocument;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.Clock;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static java.time.Clock.systemDefaultZone;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

public class MongoJobRunSemaphoreProviderTest {

    private MongoJobRunSemaphoreProvider jobRunSemaphoreProvider;
    private MongoJobRunSemaphoreRepository jobRunSemaphoreRepository;
    private Clock clock;

    @BeforeMethod
    public void setUp() throws Exception {
        clock = TestClock.now();
        jobRunSemaphoreRepository = mock(MongoJobRunSemaphoreRepository.class);
        jobRunSemaphoreProvider = new MongoJobRunSemaphoreProvider(jobRunSemaphoreRepository, clock);

    }

    @Test
    public void aquireAllSemaphores() {
        // given
        Set<String> jobTypes = new HashSet<String>(Arrays.asList("jobA", "jobB"));
        final JobRunSemaphore jobA = new JobRunSemaphore("jobA", OffsetDateTime.now(clock));
        final JobRunSemaphore jobB = new JobRunSemaphore("jobB", OffsetDateTime.now(clock));
        when(jobRunSemaphoreRepository.create(jobA)).thenReturn(jobA);
        when(jobRunSemaphoreRepository.create(jobB)).thenReturn(jobB);

        // when
        final boolean runSemaphoresForJobTypes = jobRunSemaphoreProvider.getRunSemaphoresForJobTypes(jobTypes);

        // then
        assertThat(runSemaphoresForJobTypes, is(true));
        verify(jobRunSemaphoreRepository, times(1)).create(jobA);
        verify(jobRunSemaphoreRepository, times(1)).create(jobB);
    }

    @Test
    public void CantAquireSemaphoreBecauseItAlreadyAquired() {
        // given
        Set<String> jobTypes = new HashSet<String>(Arrays.asList("jobA"));
        final JobRunSemaphore jobA = new JobRunSemaphore("jobA", OffsetDateTime.now(clock));
        when(jobRunSemaphoreRepository.create(jobA))
                .thenReturn(jobA)
                .thenThrow(new MongoWriteException(new WriteError(11000,"", BsonDocument.parse("{}")), new ServerAddress()));

        // when
        final boolean runSemaphoresForJobTypes = jobRunSemaphoreProvider.getRunSemaphoresForJobTypes(jobTypes);
        final boolean runSemaphoresForJobTypes2 = jobRunSemaphoreProvider.getRunSemaphoresForJobTypes(jobTypes);

        // then
        assertThat(runSemaphoresForJobTypes, is(true));
        assertThat(runSemaphoresForJobTypes2, is(false));
        verify(jobRunSemaphoreRepository, times(2)).create(jobA);
    }

    @Test
    public void aquireOnlySomeSemaphoresAndReleaseThemAgain() {
        // given
        Set<String> jobTypes1 = new HashSet<>(Arrays.asList("jobB"));
        Set<String> jobTypes2 = new HashSet<>(Arrays.asList("jobC", "jobB", "jobA"));
        final JobRunSemaphore jobA = new JobRunSemaphore("jobA", OffsetDateTime.now(clock));
        final JobRunSemaphore jobB = new JobRunSemaphore("jobB", OffsetDateTime.now(clock));
        final JobRunSemaphore jobC = new JobRunSemaphore("jobC", OffsetDateTime.now(clock));
        when(jobRunSemaphoreRepository.create(jobA))
                .thenReturn(jobA);
        when(jobRunSemaphoreRepository.create(jobB))
                .thenReturn(jobB)
                .thenThrow(new MongoWriteException(new WriteError(11000,"", BsonDocument.parse("{}")), new ServerAddress()));
        when(jobRunSemaphoreRepository.create(jobC))
                .thenReturn(jobA);

        // when
        final boolean runSemaphoresForJobTypes = jobRunSemaphoreProvider.getRunSemaphoresForJobTypes(jobTypes1);
        final boolean runSemaphoresForJobTypes2 = jobRunSemaphoreProvider.getRunSemaphoresForJobTypes(jobTypes2);

        // then
        assertThat(runSemaphoresForJobTypes, is(true));
        assertThat(runSemaphoresForJobTypes2, is(false));
        verify(jobRunSemaphoreRepository, times(1)).create(jobA);
        verify(jobRunSemaphoreRepository, times(2)).create(jobB);
        verify(jobRunSemaphoreRepository, times(0)).create(jobC);
        verify(jobRunSemaphoreRepository, times(1)).delete(jobA.getId());
    }


}