-- Tarantool for the memstash integration tests: a bounded memtx arena, a dedicated user and the cache space the
-- tarantool_adapter expects ([key string, value varbinary, expire_at unsigned], primary index on the key).
box.cfg{
    listen = 3301,
    memtx_memory = 384 * 1024 * 1024,
    net_msg_max = 1024,
    log_level = 5,
}

box.schema.user.create('memstash', {password = 'memstash', if_not_exists = true})
box.schema.user.grant('memstash', 'read,write,execute', 'universe', nil, {if_not_exists = true})

local space = box.schema.space.create('memstash_cache', {if_not_exists = true})
space:format({
    {name = 'key', type = 'string'},
    {name = 'value', type = 'varbinary'},
    {name = 'expire_at', type = 'unsigned'},
})
space:create_index('primary', {parts = {'key'}, if_not_exists = true})
