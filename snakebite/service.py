# -*- coding: utf-8 -*-
# Copyright (c) 2013 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

import socket
import logging
import errno
from functools import wraps

import google.protobuf.service as service

from snakebite.channel import SocketRpcChannel
from snakebite.errors import RequestError, OutOfNNException

log = logging.getLogger(__name__)

class RpcService(object):
    def __init__(self, service_stub_class, port, host, hadoop_version, effective_user=None):
        self.service_stub_class = service_stub_class
        self.port = port
        self.host = host
        self.hadoop_version = hadoop_version
        self.channel = SocketRpcChannel(host=self.host,
                                        port=self.port,
                                        version=hadoop_version,
                                        effective_user=effective_user)
        # Setup the RPC channel
        self.service = self.service_stub_class(self.channel)
        self._decorate_with_service_methods(service_stub_class)
        self.controller = SocketRpcController()

    def _decorate_with_service_methods(self, service_stub_class):
        # Go through service_stub methods and add a wrapper function to
        # this object that will call the method
        for method in service_stub_class.GetDescriptor().methods:
            # Add service methods to the this object
            rpc = lambda request, service=self, method=method.name: service.call(service_stub_class.__dict__[method], request)

            self.__dict__[method.name] = rpc

    def call(self, method, request):
        self.controller.reset()
        return method(self.service, self.controller, request)

    def __str__(self):
        return "Rpc service to %s:%d" % (self.host, self.port)


class HARpcService(RpcService):
    def __init__(self, service_stub_class, namenodes, effective_user=None):
        self._namenodes = namenodes
        self._stub_class = service_stub_class

        self.services = tuple(RpcService(service_stub_class,
                                         nn.port,
                                         nn.host,
                                         nn.version,
                                         effective_user) for nn in namenodes)
        self.active_service = None
        self._decorate_with_service_methods(service_stub_class)

    def __handle_request_error(self, service, exception):
        log.debug("Request failed with %s" % exception)
        if exception.args[0].startswith("org.apache.hadoop.ipc.StandbyException"):
            service.controller.reason = "standby"
            pass
        else:
            # There's a valid NN in active state, but there's still request error - raise
            service.controller.reason = "namenode error"
            raise

    def __handle_socket_error(self, service, exception):
        log.debug("Request failed with %s" % exception)
        if exception.errno in (errno.ECONNREFUSED, errno.EHOSTUNREACH):
            # if NN is down or machine is not available, pass it:
            service.controller.reason = "not available - possibly down"
            pass
        elif isinstance(exception, socket.timeout):
            # if there's communication/socket timeout, pass it:
            service.controller.reason = "request timeout"
            pass
        else:
            raise

    def _try_call_service(self, service, method, request):
        try:
            return service.call(method, request)
        except RequestError as e:
            self.__handle_request_error(service, e)
        except socket.error as e:
            self.__handle_socket_error(service, e)

    def _try_iterative_service_calls(self, method, request):
        for service in self.services:
            log.debug("Will try %s" % service)
            result = self._try_call_service(service, method, request)
            if result is None:
                log.debug("%s failed" % service)
                continue
            else:
                self.active_service = service
                return result

    def has_active_service(self):
        return self.active_service is not None

    def call(self, method, request):
        if self.active_service:
            log.debug("Try to resuse previously used active %s" % self.active_service)
            result = self._try_call_service(self.active_service, method, request)
            if result is None:
                log.debug("Previously active service %s now fails" % self.active_service)
                # Put currently failing service at the end of service tuple
                self.services = filter(lambda x: x != self.active_service, self.services)
                self.services += self.active_service,
                self.active_service = None
            else:
                return result
        log.debug("No active service to reuse - will try all services one by one")

        result = self._try_iterative_service_calls(method, request)
        if result is None:
            msg = "Request tried and failed for all %d namenodes: " % len(self._namenodes)
            for service in self.services:
                msg += "\n\t* %s:%d (reason: %s)" % (service.host, service.port, service.controller.reason)
            msg += "\nLook into debug messages - add -D flag!"
            raise OutOfNNException(msg)
        else:
            return result


class AsyncHARpcService(HARpcService):
    def __init__(self, service_stub_class, namenodes, effective_user=None):
        super(AsyncHARpcService, self).__init__(service_stub_class, namenodes, effective_user)

    def call(self, method, request):
        pass


class SocketRpcController(service.RpcController):
    ''' RpcController implementation to be used by the SocketRpcChannel class.

    The RpcController is used to mediate a single method call.
    '''

    def __init__(self):
        '''Constructor which initializes the controller's state.'''
        self._fail = False
        self._error = None
        self.reason = None

    def handleError(self, error_code, message):
        '''Log and set the controller state.'''
        self._fail = True
        self.reason = error_code
        self._error = message

    def reset(self):
        '''Resets the controller i.e. clears the error state.'''
        self._fail = False
        self._error = None
        self.reason = None

    def failed(self):
        '''Returns True if the controller is in a failed state.'''
        return self._fail

    def error(self):
        return self._error
