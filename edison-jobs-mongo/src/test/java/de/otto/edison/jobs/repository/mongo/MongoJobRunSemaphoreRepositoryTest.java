package de.otto.edison.jobs.repository.mongo;

import com.github.fakemongo.Fongo;
import com.mongodb.MongoException;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoDatabase;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.OffsetDateTime;
import java.util.*;

import static com.mongodb.ErrorCategory.DUPLICATE_KEY;
import static de.otto.edison.jobs.domain.JobMessage.jobMessage;
import static java.time.OffsetDateTime.now;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.testng.Assert.fail;

public class MongoJobRunSemaphoreRepositoryTest {

    private MongoJobRunSemaphoreRepository repo;

    @BeforeMethod
    public void setup() {
        final Fongo fongo = new Fongo("inmemory-mongodb");
        final MongoDatabase database = fongo.getDatabase("jobsinfo");
        repo = new MongoJobRunSemaphoreRepository(database);
    }

    @Test
    public void shouldStoreAndRetrieveJobInfo() {
        // given
        final OffsetDateTime now = OffsetDateTime.now();
        final JobRunSemaphore jobRunSemaphore = new JobRunSemaphore("someType", now);
        final JobRunSemaphore writtenClusterLock = repo.create(jobRunSemaphore);
        // when
        final Optional<JobRunSemaphore> foundSemaphore = repo.findOne("someType");
        // then
        assertThat(foundSemaphore.isPresent(), is(true));
        assertThat(foundSemaphore.get(), equalTo(writtenClusterLock));
    }

    @Test
    public void shouldTakeCareThatSameLockCantBeCreatedTwice() {
        // given
        final OffsetDateTime now = OffsetDateTime.now();
        final JobRunSemaphore jobRunSemaphore = new JobRunSemaphore("someType", now);
        repo.create(jobRunSemaphore);
        try {
            repo.create(jobRunSemaphore);
            fail( "Expected exception has not been thrown" );
        } catch (MongoWriteException mwe) {
            assertThat(mwe.getError().getCategory(), is(DUPLICATE_KEY));
        }
    }
}