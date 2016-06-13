package de.otto.edison.jobs.repository.mongo;

import java.time.OffsetDateTime;

public class JobRunSemaphore {
    private String id;
    private OffsetDateTime created;

    public JobRunSemaphore(String id, OffsetDateTime created) {
        this.id = id;
        this.created = created;
    }

    public String getId() {
        return id;
    }


    public OffsetDateTime getCreated() {
        return created;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        JobRunSemaphore that = (JobRunSemaphore) o;

        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        return created != null ? created.equals(that.created) : that.created == null;

    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (created != null ? created.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "JobRunSemaphore{" +
                "id='" + id + '\'' +
                ", created=" + created +
                '}';
    }
}
