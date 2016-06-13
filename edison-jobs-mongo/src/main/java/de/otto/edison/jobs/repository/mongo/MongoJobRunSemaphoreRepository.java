package de.otto.edison.jobs.repository.mongo;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import de.otto.edison.mongo.AbstractMongoRepository;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;


import static de.otto.edison.jobs.repository.mongo.DateTimeConverters.toDate;
import static de.otto.edison.jobs.repository.mongo.DateTimeConverters.toOffsetDateTime;
import static de.otto.edison.jobs.repository.mongo.JobRunSemaphoreStructure.*;

@Repository(value = "jobRunSemaphoreRepository")
public class MongoJobRunSemaphoreRepository extends AbstractMongoRepository<String, JobRunSemaphore>  {

    private static final String COLLECTION_NAME = "jobRunSemaphore";

    private final MongoCollection<Document> collection;

    @Autowired
    public MongoJobRunSemaphoreRepository(final MongoDatabase database) {
        this.collection = database.getCollection(COLLECTION_NAME);
    }

    @Override
    protected void ensureIndexes() {

    }

    @Override
    protected MongoCollection<Document> collection() {
        return collection;
    }

    @Override
    protected String keyOf(JobRunSemaphore value) {
        return value.getId();
    }

    @Override
    protected Document encode(JobRunSemaphore value) {
        return new Document()
                .append(JobRunSemaphoreStructure.ID.key(), value.getId())
                .append(CREATED.key(), toDate(value.getCreated()));
    }

    @Override
    protected JobRunSemaphore decode(Document document) {
        return new JobRunSemaphore(
                document.getString(JobRunSemaphoreStructure.ID.key()),
                toOffsetDateTime(document.getDate(CREATED.key())));
    }


}
