import redis
import subprocess
import sys
import json

ROLE_MASTER = 'master'
ROLE_SLAVE = 'slave'

STR_START = 'answer:'
STR_END = 'Authoritative'

FILTER_REDIS = 'redis'
FILTER_REDIS_ = 'redis_'
FILTER_VM_REDIS = 'vm-redis'


def _get_domains_by_hostname(ip):
    cmd = 'nslookup ' + ip
    child = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    out = child.communicate()
    start = out[0].find(STR_START)
    end = out[0].rfind(STR_END)
    ip_domains = out[0][start + len(STR_START):end].strip('\n').splitlines()
    _result = []
    for ip_domain in ip_domains:
        start = ip_domain.find(' = ') + len(' = ')
        end = len(ip_domain) - 1
        _result.append(ip_domain[start:end])
    return _result


def _get_hostname_by_ip(ip):
    hosts = _get_domains_by_hostname(ip)
    for _host in hosts:
        if (_host.startswith(FILTER_REDIS) or _host.startswith(FILTER_VM_REDIS)) and not _host.startswith(
                FILTER_REDIS_):
            return _host


def find_relationship(master_host, master_port, _result={}):
    """
    Use recursion loop to retrieve the master slave chain.
    The result is a dict collection. The key is slave_host:port, and the value is master_host:port.
    To the master node, a little special, the key is master_host:port, the key is 'master'.
    """
    handler = redis.StrictRedis(master_host, master_port)
    info = handler.info()
    connected_slaves = info['connected_slaves']
    if ROLE_MASTER == info['role']:
        key = '%s:%s' % (master_host, master_port)
        _result[key] = 'master'
        if 0 == connected_slaves:
            return _result

    if 0 == connected_slaves and ROLE_MASTER != info['role']:
        # recursion end
        return
    else:
        for index in range(0, connected_slaves):
            slave_info = info['slave' + str(index)]
            slave_ip = slave_info.split(',')[0]
            slave_port = slave_info.split(',')[1]
            slave_host = _get_hostname_by_ip(slave_ip)
            key = '%s:%s' % (slave_host, slave_port)
            _result[key] = '%s:%s' % (master_host, master_port)
            find_relationship(slave_host, int(slave_port), _result)


def find_master(_host, _port, _master_list):
    handler = redis.StrictRedis(_host, _port)
    try:
        info = handler.info()
    except redis.exceptions.ConnectionError as detail:
        _master_list.append(('error', 'find_master error: %s' % detail))
        return _master_list

    _master_list.append((_host, _port))
    role = info['role']
    if ROLE_MASTER == role:
        return _master_list
    elif ROLE_SLAVE == role:
        master_host = info['master_host']
        master_port = info['master_port']
        return find_master(master_host, master_port, _master_list)


if '__main__' == __name__:
    if 3 != len(sys.argv):
        print json.dumps({'error': 'The usage should be:python master_slave host port'})
        exit(0)
    host = sys.argv[1]
    port = int(sys.argv[2])
    master_list = []
    master_info = find_master(host, port, master_list)
    master_host_port = master_list[len(master_list) - 1]
    if 'error' == master_host_port[0]:
        print json.dumps({'error': str(master_list)})
    else:
        result = {}
        find_relationship(master_host_port[0], master_host_port[1], result)
        print json.dumps(result)