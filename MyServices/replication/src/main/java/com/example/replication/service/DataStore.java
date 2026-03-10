package com.example.replication.service;

import org.springframework.stereotype.Service;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class DataStore {
    private final AtomicLong version = new AtomicLong(0);
    private final AtomicLong value = new AtomicLong(0);

    public Record get() {
        return new Record(value.get(), version.get());
    }

    public boolean updateIfNewer(long newValue, long newVersion) {
        // Синхронизация по версии: обновляем только если версия >= текущей
        synchronized (this) {
            if (newVersion >= version.get()) {
                version.set(newVersion);
                value.set(newValue);
                return true;
            }
            return false;
        }
    }

    public record Record(long value, long version) {}
}