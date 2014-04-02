__author__ = 'fowler'
import socket
import xmlrpclib
import sys
import json

OPERATION_START_GROUP = 'start_g'
OPERATION_STOP_GROUP = 'stop_g'
OPERATION_LOAD_GROUP = 'load_g'
OPERATION_START_INSTANCE = 'start_i'
OPERATION_STOP_INSTANCE = 'stop_i'
OPERATION_LOAD_INSTANCE = 'load_i'
OPERATION_ACTIVE = 'active'
OPERATION_REMOVE = 'remove'
OPERATION_SHUTDOWN = 'shutdown'

STATUS_OK = 0
STATUS_NEW = 1
STATUS_CHANGED = 2
STATUS_REMOVED = 3


def get_supervisor_server():
    hostname = socket.getfqdn(socket.gethostname())
    ip = socket.gethostbyname(hostname)
    return xmlrpclib.Server('http://' + ip + ':9001/RPC2')


def start_supervisor_group(_server, _group_name):
    try:
        _server.supervisor.startProcessGroup(_group_name)
        return 'Supervisor group %s is started successfully.' % _group_name
    except xmlrpclib.Fault as detail:
        return 'Start supervisor group failed: %s' % detail


def start_supervisor_instance(_server, _group_name, _instance):
    try:
        process = _group_name + ':' + _instance
        _server.supervisor.startProcess(process)
        return 'Supervisor instance %s is started successfully.' % process
    except xmlrpclib.Fault as detail:
        return 'Start supervisor instance failed: %s' % detail


def stop_supervisor_group(_server, _group_name):
    try:
        _server.supervisor.stopProcessGroup(_group_name)
        return 'Supervisor group %s is stopped successfully.' % _group_name
    except xmlrpclib.Fault as detail:
        return 'Stop supervisor group failed: %s' % detail


def stop_supervisor_instance(_server, _group_name, _instance):
    try:
        process = _group_name + ':' + _instance
        _server.supervisor.stopProcess(process)
        return 'Supervisor instance %s is stopped successfully.' % process
    except xmlrpclib.Fault as detail:
        return 'Stop supervisor instance failed: %s' % detail


def remove_supervisor_group(_server, _group_name):
    try:
        _server.supervisor.removeProcessGroup(_group_name)
        return 'Supervisor group %s is removed successfully.' % _group_name
    except xmlrpclib.Fault as detail:
        return 'Remove supervisor group failed: %s' % detail


def get_all_supervisor_groups(_server):
    groups = _server.supervisor.getAllConfigInfo()
    result = {}
    for group in groups:
        result[group['group']] = STATUS_OK
    reload_result = _server.supervisor.reloadConfig()
    news = reload_result[0][0]
    changes = reload_result[0][1]
    removes = reload_result[0][2]
    for new in news:
        result[new] = STATUS_NEW
    for change in changes:
        result[change] = STATUS_CHANGED
    for remove in removes:
        result[remove] = STATUS_REMOVED
    return result


def get_all_supervisor_instances(_server):
    processes = _server.supervisor.getAllProcessInfo()
    result = []
    for process in processes:
        item = {'group': process['group'], 'name': process['name'], 'description': process['description'],
                'statename': process['statename']}
        result.append(item)
    return json.dumps(result)


def active_supervisor_group(_server, _group_name):
    result = _server.supervisor.reloadConfig()
    if 0 == len(result[0][0]):
        return 'No group can be activated.'
    elif _group_name not in result[0][0]:
        return '%s is not a new group.' % _group_name
    else:
        try:
            _server.supervisor.addProcessGroup(_group_name)
            return 'Supervisor group %s is activated successfully.' % _group_name
        except xmlrpclib.Fault as detail:
            return 'Active supervisor group failed: %s' % detail


def stop_supervisor(_server):
    try:
        _server.supervisor.shutdown()
        return 'Supervisor is stopped successfully.'
    except socket.error as detail:
        return 'Stop supervisor failed: %s' % detail


if "__main__" == __name__:
    server = get_supervisor_server()
    if OPERATION_START_GROUP == sys.argv[1]:
        print json.dumps(start_supervisor_group(server, sys.argv[2]))
    elif OPERATION_START_INSTANCE == sys.argv[1]:
        print json.dumps(start_supervisor_instance(server, sys.argv[2], sys.argv[3]))
    elif OPERATION_STOP_GROUP == sys.argv[1]:
        print json.dumps(stop_supervisor_group(server, sys.argv[2]))
    elif OPERATION_STOP_INSTANCE == sys.argv[1]:
        print json.dumps(stop_supervisor_instance(server, sys.argv[2], sys.argv[3]))
    elif OPERATION_ACTIVE == sys.argv[1]:
        print json.dumps(active_supervisor_group(server, sys.argv[2]))
    elif OPERATION_REMOVE == sys.argv[1]:
        print json.dumps(remove_supervisor_group(server, sys.argv[2]))
    elif OPERATION_LOAD_GROUP == sys.argv[1]:
        print json.dumps(get_all_supervisor_groups(server))
    elif OPERATION_LOAD_INSTANCE == sys.argv[1]:
        print json.dumps(get_all_supervisor_instances(server))
    elif OPERATION_SHUTDOWN == sys.argv[1]:
        print json.dumps(stop_supervisor(server))
    else:
        print json.dumps('Not supported operation.')