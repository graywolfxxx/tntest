#!/usr/bin/env tarantool

local uuid  = require('uuid')
local fio   = require('fio')
local clock = require('clock')
local fiber = require('fiber')

local test_name  = 'test01'
local space_name = test_name

local function init (sock_fn)
    -- global cleanup
    fio.unlink(sock_fn)
    fio.unlink(fio.cwd() .. '/vinyl.meta')
    return {dir = fio.tempdir(), sock_fn = sock_fn}
end

local function finish (meta)
    if meta == nil or meta.dir == nil or meta.sock_fn == nil then
        log.error('Wrong meta in "finish"')
        os.exit(-1)
    end
    local files = fio.glob(fio.pathjoin(meta.dir, '*'))
    for _, file in pairs(files) do
        fio.unlink(file)
    end
    fio.rmdir(meta.dir)
    fio.unlink(meta.sock_fn)
    fio.unlink(fio.cwd() .. '/vinyl.meta')
    os.exit(0)
end

-- Init Tarantool's environmetn: users, grants and so on
local function tnt_bootstrap ()
    box.schema.user.create(test_name .. '_user', {password = '123'})
    box.schema.user.grant(test_name .. '_user', 'read,write,execute', 'universe', nil)
    box.schema.user.grant('guest', 'read,write,execute', 'universe')
end

local function bench (f)
    local _tm = clock.proc()
    local res = f()
    return res, clock.proc() - _tm
end

local function create_test_schema ()
    local test_space = box.space[space_name]
    if test_space == nil and box.cfg.read_only == false then
        test_space = box.schema.create_space(space_name, {temporary = false, if_not_exists = true})
        test_space:create_index('primary',  {parts = {1, 'UNSIGNED'}, type = 'tree', unique = true, if_not_exists = true})
    end
end

local function drop_test_schema ()
    if box.space[space_name] ~= nil then
        box.space[space_name]:drop()
    end
end


local meta = init('/tmp/tnt_' .. test_name .. '.sock')

box.cfg({
    slab_alloc_arena = 0.5,
    wal_dir          = meta.dir,
    snap_dir         = meta.dir,
    snapshot_period  = 0,
    wal_mode         = 'none',
    listen           = 'unix/:' .. meta.sock_fn,
    logger           = fio.pathjoin(meta.dir, 'tarantool.log')
})

box.once(test_name, tnt_bootstrap)

create_test_schema()

local blank_table = {}
for i = 1, 12 do
    table.insert(blank_table, 0)
end

local docs = {}
for i = 1, 15 do
    table.insert(docs, uuid.bin())
end

local res, time = bench(function ()
    local the_time   = fiber.time()
    local cur_day_ts = math.floor(the_time - math.fmod(the_time, 86400))
    local id = 1ULL
    for _, doc_uuid in pairs(docs) do
        for _, mask in pairs({1, 2, 4}) do
            for tm = cur_day_ts - 3 * 86400, cur_day_ts, 60 do
                box.space[space_name]:insert({id, tm, doc_uuid, mask, unpack(blank_table)})
                id = id + 1
            end
        end
    end
    return true
end)

local tests = {}
for i = 1, #arg do
    local test_num = tonumber(arg[i])
    if test_num ~= nil then
        if test_num == 0 then
            local iter_time
            _, iter_time = bench(function ()
                for _, f in box.space[space_name].index.primary:pairs(nil, {iterator = box.index.GT}) do
                end
                return 0
            end)
            print(string.format('Create %d facts in %0.5f sec', box.space[space_name]:count(), time))
            print(string.format('Empty iterator: %0.5f sec', iter_time))
        elseif test_num == 1 then
            table.insert(tests, {idx = 2,  use_unpack = false, res = {min_time = 100000.0, max_time = 0.0, times = {}}})
        elseif test_num == 2 then
            table.insert(tests, {idx = 2,  use_unpack = true,  res = {min_time = 100000.0, max_time = 0.0, times = {}}})
        elseif test_num == 3 then
            table.insert(tests, {idx = 16, use_unpack = false, res = {min_time = 100000.0, max_time = 0.0, times = {}}})
        elseif test_num == 4 then
            table.insert(tests, {idx = 16, use_unpack = true,  res = {min_time = 100000.0, max_time = 0.0, times = {}}})
        end
    end
end

for x = 1, 2 do
    if x == 4 and #tests == 4 then
        tests[1], tests[2] = tests[2], tests[1]
        tests[3], tests[4] = tests[4], tests[3]
    end
    for i = 1, #tests do
        local idx, use_unpack, res = tests[i]['idx'], tests[i]['use_unpack'], tests[i]['res']
        local time
        collectgarbage('collect')
        if use_unpack then
            _, time = bench(function ()
                for _, f in box.space[space_name].index.primary:pairs(nil, {iterator = box.index.GE}) do
                    local a, b = f:unpack(idx - 1, idx)
                end
                return 0
            end)
        else
            _, time = bench(function ()
                for _, f in box.space[space_name].index.primary:pairs(nil, {iterator = box.index.GE}) do
                    local a, b = f[idx - 1], f[idx]
                end
                return 0
            end)
        end
        if res['min_time'] > time then
            res['min_time'] = time
        end
        if res['max_time'] < time then
            res['max_time'] = time
        end
        table.insert(res['times'], time)
    end
end

for i = 1, #tests do
    local idx, use_unpack, res = tests[i]['idx'], tests[i]['use_unpack'], tests[i]['res']
    local res_str
    if (res['max_time'] - res['min_time']) < 0.001 then
        res_str = string.format('%0.5f sec', res['max_time'])
    else
        res_str = string.format('min: %0.5f sec, max: %0.5f sec', res['min_time'], res['max_time'])
    end
    if idx < 10 then
        res_str = '  ' .. res_str
    elseif idx < 11 then
        res_str = ' ' .. res_str
    end
    if use_unpack then
        print(string.format('a, b = tuple:unpack(%d, %d): %s', idx - 1, idx, res_str))
    else
        print(string.format('a, b = tuple[%d], tuple[%d]: %s', idx - 1, idx, res_str))
    end
end

drop_test_schema()
finish(meta)
