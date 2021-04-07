function remove_old_object(rec, ttl)
    if record.ttl(rec) <= ttl then
        info("Delete TTL %s / TTL record %s", tostring(ttl), tostring(record.ttl(rec)))
        aerospike:remove(rec)
    end
end