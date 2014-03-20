import os
import sys
import ConfigParser
import socket
import xmlrpclib

REDIS_TEMPLATE = """daemonize no
pidfile /home/server/redis/log/redis_%(port)s.pid
port %(port)s
bind 0.0.0.0
timeout 0
tcp-keepalive 0
loglevel notice
logfile /home/server/redis/log/redis_%(port)s.log
databases 16
save 864000000 1
stop-writes-on-bgsave-error no
rdbcompression yes
rdbchecksum yes
dbfilename dump_%(port)s.rdb
dir /home/server/redis/data/
slave-serve-stale-data yes
slave-read-only no
repl-disable-tcp-nodelay no
slave-priority 100
maxclients 100000
rename-command KEYS FB38E1
rename-command FLUSHALL ""
rename-command FLUSHDB ""
appendonly no
appendfsync everysec
no-appendfsync-on-rewrite no
auto-aof-rewrite-percentage 100
auto-aof-rewrite-min-size 64mb
lua-time-limit 5000
slowlog-log-slower-than 10000
slowlog-max-len 1024
hash-max-ziplist-entries 512
hash-max-ziplist-value 64
list-max-ziplist-entries 512
list-max-ziplist-value 64
set-max-intset-entries 512
zset-max-ziplist-entries 128
zset-max-ziplist-value 64
activerehashing yes
client-output-buffer-limit normal 0 0 0
client-output-buffer-limit slave 256mb 64mb 60
client-output-buffer-limit pubsub 32mb 8mb 60
hz 10
maxmemory %(maxmemory)s
maxmemory-policy %(maxmemory_policy)s
"""

TYPE_MUTUAL_PRIOR = '00'
TYPE_MUTUAL_POSTERIOR = '01'
TYPE_INDEPENDENT = '1'
TYPE_MASTER_ONLY = '2'
TYPE_SLAVE_ONLY = '3'

REDIS_CONFIG_PATH = "/home/server/redis/etc/redis_%s.conf"
SUPERVISOR_CONFIG_PATH = "/home/server/supervisor/etc/%s.supervisor"

SUCCESS = "Success"


def get_supervisor_server():
    hostname = socket.getfqdn(socket.gethostname())
    ip = socket.gethostbyname(hostname)
    return xmlrpclib.Server('http://' + ip + ':9001/RPC2')


def is_supervisor_group_exist_and_running(server, group_name):
    groups = server.supervisor.getAllConfigInfo()
    group_names = set()
    for group in groups:
        group_names.add(group['group'])
    return group_name in group_names


def is_redis_config_exist(_start, _end):
    for port in range(int(_start), int(_end)):
        redis_conf = REDIS_CONFIG_PATH % (port,)
        return os.path.isfile(redis_conf)


def generate_node_conf(_file_name, _port, _master, _max_memory, _max_memory_policy):
    if os.path.isfile(_file_name):
        os.remove(_file_name)
    required = open(_file_name, "w")
    try:
        required.write(REDIS_TEMPLATE % {
            "port": _port,
            "maxmemory": _max_memory,
            "maxmemory_policy": _max_memory_policy,
        })
        if 0 != len(_master):
            required.write("slaveof " + _master + " " + str(_port) + '\n')
    finally:
        required.close()


def generate_super_config(_prefix, _start, _end):
    file_name = SUPERVISOR_CONFIG_PATH % (_prefix,)
    section_new = "program:%s" % (_prefix,)
    parser = ConfigParser.ConfigParser()
    parser.add_section(section_new)
    parser.set(section_new, "numprocs", int(_end) - int(_start) + 1)
    parser.set(section_new, "process_name", "%(process_num)04d")
    parser.set(section_new, "numprocs_start", _start)
    parser.set(section_new, "command", "redis-server /home/server/redis/etc/redis_%(process_num)04d.conf")
    parser.set(section_new, "autostart", "false")
    parser.set(section_new, "autorestart", "true")
    parser.set(section_new, "stdout_logfile", "NONE")
    parser.set(section_new, "stderr_logfile",
               "/home/server/redis/log/redis_%(process_num)04d-stderr.log")
    parser.set(section_new, "stdout_logfile_maxbytes", "500MB")
    parser.set(section_new, "stdout_logfile_backups", 50)
    parser.set(section_new, "stdout_capture_maxbytes", "1MB")
    parser.set(section_new, "stdout_events_enabled", "false")
    parser.set(section_new, "loglevel", "info")
    parser.set(section_new, "priority", 1100)
    if os.path.isfile(file_name):
        os.remove(file_name)
    parser.write(open(file_name, "w"))


def setup_redis_shard(start, end, max_memory='0', max_memory_policy='volatile-lru', master=''):
    for port in range(int(start), int(end) + 1):
        redis_conf = REDIS_CONFIG_PATH % (port,)
        generate_node_conf(redis_conf, port, master, max_memory, max_memory_policy)


def setup_supervisor(end, prefix, server, start):
    generate_super_config(prefix, start, end)
    result = server.supervisor.reloadConfig()
    if 0 < len(result[0][0]) and prefix in result[0][0]:
        server.supervisor.addProcessGroup(prefix)
        return SUCCESS


def setup_redis_cluster(cluster_type, start, end, max_memory, max_memory_policy, master):
    if TYPE_MUTUAL_PRIOR == cluster_type:
        mid = (int(start) + int(end)) / 2
        setup_redis_shard(start, mid, max_memory, max_memory_policy, '')
        setup_redis_shard(mid + 1, end, max_memory, max_memory_policy, master)
        return SUCCESS
    elif TYPE_MUTUAL_POSTERIOR == cluster_type:
        mid = (int(start) + int(end)) / 2
        setup_redis_shard(start, mid, max_memory, max_memory_policy, master)
        setup_redis_shard(mid + 1, end, max_memory, max_memory_policy, '')
        return SUCCESS
    elif TYPE_INDEPENDENT == cluster_type:
        setup_redis_shard(start, end, max_memory, max_memory_policy, master)
        return SUCCESS
    elif TYPE_MASTER_ONLY == cluster_type:
        setup_redis_shard(start, end, max_memory, max_memory_policy, '')
        return SUCCESS
    elif TYPE_SLAVE_ONLY == cluster_type:
        setup_redis_shard(start, end, max_memory, max_memory_policy, master)
        return SUCCESS
    else:
        return "Cluster type %s is not supported." % (cluster_type,)


def run(cluster_type, start, end, prefix, max_memory='0', max_memory_policy='volatile-lru', master=''):
    server = get_supervisor_server()

    if is_supervisor_group_exist_and_running(server, prefix):
        return prefix + ' group is running on ' + socket.getfqdn(socket.gethostname())
    elif is_redis_config_exist(start, end):
        return "redis config files exist on " + socket.getfqdn(socket.gethostname())
    else:
        redis_result = setup_redis_cluster(cluster_type, start, end, max_memory, max_memory_policy, master)
        super_result = setup_supervisor(end, prefix, server, start)
        if SUCCESS == redis_result and SUCCESS == super_result:
            return SUCCESS
        else:
            print "redis: %s, supervisor: %s" % (redis_result, super_result)


if '__main__' == __name__:
    if 4 > len(sys.argv):
        print "redis port is required. parameter sequence is: port master maxmemory maxmemory-policy"
        exit(1)
    print run(*sys.argv[1:])