# coding: UTF-8
'''
Created on 20140310

@author: mingliu
'''
from thrift.protocol import TBinaryProtocol, TJSONProtocol, TCompactProtocol
from thrift.transport import TSocket, TTransport, THttpClient, TZlibTransport

class ThriftClient(object):
    '''
    classdocs
    '''
    __protocol_type = {
                        "tbinary":TBinaryProtocol.TBinaryProtocol,
                        "taccel":TBinaryProtocol.TBinaryProtocolAccelerated,
                        "tcompact":TCompactProtocol.TCompactProtocol,
                        "tjson":TJSONProtocol.TJSONProtocol
                        }

    def __init__(self, ip="127.0.0.1", port="9000", transport="tsocket", protocol="tbinary", timeout=10000):
        '''
        Constructor
        '''
        self._ip = ip
        self._port = int(port)
        self._transport = transport
        self._protocol = protocol
        self._timeout = timeout
        self.__initialize()

    def __initialize(self):
        if self._transport == 'http':
#             self._transport = THttpClient.THttpClient(self._ip, self._port)
            self._transport = THttpClient.THttpClient('http://%s:%d/' %(self._ip, self._port))
        else:
            socket = TSocket.TSocket(self._ip, self._port)
            socket.setTimeout(self._timeout)
            if self._transport == 'frame':
                self._transport = TTransport.TFramedTransport(socket)
            else:
                self._transport = TTransport.TBufferedTransport(socket)
                if self._transport == 'zlib':
                    self._transport = TZlibTransport.TZlibTransport(self._transport)


        self._protocol = ThriftClient.__protocol_type[self._protocol](self._transport)

    def get_protocol(self):
        '''
        get the transport handler for client
        '''
        return self._protocol
    
    def connect(self):
        '''
        connect to the thrift rpc server
        '''
        self._transport.open()
    
    def close(self):
        '''
        close the connection to the thrift server
        '''
        self._transport.close()
