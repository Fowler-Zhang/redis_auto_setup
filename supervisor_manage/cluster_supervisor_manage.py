__author__ = 'fowler'
import socket
import xmlrpclib
import sys

OPERATION_START = 'start'
OPERATION_STOP = 'stop'
OPERATION_LOAD_GROUP = 'load_g'
OPERATION_ACTIVE = 'active'
OPERATION_REMOVE = 'remove'

STATUS_OK = 0
STATUS_NEW = 1
STATUS_CHANGED = 2
STATUS_REMOVED = 3


def get_supervisor_server():
    hostname = socket.getfqdn(socket.gethostname())
    ip = socket.gethostbyname(hostname)
    return xmlrpclib.Server('http://' + ip + ':9001/RPC2')


def start_cluster_supervisor(_server, _group_name):
    try:
        _server.supervisor.startProcessGroup(_group_name)
        return 'Start cluster %s supervisor success.' % _group_name
    except xmlrpclib.Fault as detail:
        return 'Start cluster supervisor error: %s' % detail


def stop_cluster_supervisor(_server, _group_name):
    try:
        _server.supervisor.stopProcessGroup(_group_name)
        return 'Stop cluster %s supervisor success.' % _group_name
    except xmlrpclib.Fault as detail:
        return 'Stop cluster supervisor error: %s' % detail


def remove_cluster_supervisor(_server, _group_name):
    try:
        _server.supervisor.removeProcessGroup(_group_name)
        return 'Remove cluster %s supervisor success.' % _group_name
    except xmlrpclib.Fault as detail:
        return 'Remove cluster supervisor error: %s' % detail


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


def active_supervisor_group(_server, _group_name):
    result = _server.supervisor.reloadConfig()
    if 0 == len(result[0][0]):
        return 'No group can be activated.'
    elif _group_name not in result[0][0]:
        return '%s is not a new group.' % _group_name
    else:
        try:
            _server.supervisor.addProcessGroup(_group_name)
            return 'Active cluster %s supervisor success.' % _group_name
        except xmlrpclib.Fault as detail:
            return 'Active cluster supervisor error: %s' % detail


if "__main__" == __name__:
    server = get_supervisor_server()
    if OPERATION_START == sys.argv[1]:
        print start_cluster_supervisor(server, sys.argv[2])
    elif OPERATION_STOP == sys.argv[1]:
        print stop_cluster_supervisor(server, sys.argv[2])
    elif OPERATION_ACTIVE == sys.argv[1]:
        print active_supervisor_group(server, sys.argv[2])
    elif OPERATION_REMOVE == sys.argv[1]:
        print remove_cluster_supervisor(server, sys.argv[2])
    elif OPERATION_LOAD_GROUP == sys.argv[1]:
        print get_all_supervisor_groups(server)
    else:
        print 'Not supported operation.'