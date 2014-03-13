import os
import ConfigParser
import sys
import socket
import xmlrpclib

REDIS_TEMPLATE = """
daemonize no
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
            required.write("slaveof " + _master + " " + _port + '\n')
    finally:
        required.close()


def generate_super_config(_file_name, _port):
    section_new = "program:redis_" + _port
    parser = ConfigParser.ConfigParser()
    parser.add_section(section_new)
    parser.set(section_new, "command", "redis-server /home/server/redis/etc/redis_" + _port + ".conf")
    parser.set(section_new, "autostart", "false")
    parser.set(section_new, "autorestart", "true")
    parser.set(section_new, "stdout_logfile", "NONE")
    parser.set(section_new, "stderr_logfile",
               "/home/server/supervisor/log/redis_" + _port + "-stderr.log")
    parser.set(section_new, "stdout_logfile_maxbytes", "500MB")
    parser.set(section_new, "stdout_logfile_backups", 50)
    parser.set(section_new, "stdout_capture_maxbytes", "1MB")
    parser.set(section_new, "stdout_events_enabled", "false")
    parser.set(section_new, "loglevel", "info")
    parser.set(section_new, "priority", 1100)
    if os.path.isfile(_file_name):
        os.remove(_file_name)
    parser.write(open(_file_name, "w"))


def get_supervisor_server():
    hostname = socket.getfqdn(socket.gethostname())
    ip = socket.gethostbyname(hostname)
    return xmlrpclib.Server('http://' + ip + ':9001/RPC2')


def is_supervisor_process_exist(server, group_name):
    processes = server.supervisor.getAllProcessInfo()
    process_names = set()
    for process in processes:
        process_names.add(process['name'])
    return group_name in process_names


def run(port, master='', max_memory='0', max_memory_policy='volatile-lru'):
    redis_conf = "/home/server/redis/etc/redis_" + port + ".conf"
    super_conf = "/home/server/supervisor/etc/redis_" + port + ".supervisor"
    group_name = 'redis_' + port
    server = get_supervisor_server()

    if os.path.isfile(redis_conf):
        return redis_conf + " is exist on " + socket.getfqdn(socket.gethostname())
    elif os.path.isfile(super_conf):
        return super_conf + " is exist on " + socket.getfqdn(socket.gethostname())
    elif is_supervisor_process_exist(server, group_name):
        group_info = server.supervisor.getProcessInfo(group_name)
        if 0 != int(group_info['pid']):
            return group_name + ' is running on ' + socket.getfqdn(socket.gethostname())
    else:
        generate_node_conf(redis_conf, port, master, max_memory, max_memory_policy)
        generate_super_config(super_conf, port)
        result = server.supervisor.reloadConfig()
        if 0 < len(result[0][0]) and group_name in result[0][0]:
            server.supervisor.addProcessGroup(group_name)
        return 'Success'


if '__main__' == __name__:
    if 1 > len(sys.argv):
        print "redis port is required. parameter sequence is: port master maxmemory maxmemory-policy"
        exit(1)
    print run(*sys.argv[1:])