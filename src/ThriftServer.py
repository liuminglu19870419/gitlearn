# coding: UTF-8
'''
Created on 20140310

@author: mingliu
'''
from thrift.transport import TSocket, TZlibTransport, TTransport
from thrift.server import TServer, THttpServer, TNonblockingServer
from thrift.protocol import TBinaryProtocol, TCompactProtocol, TJSONProtocol
from thrift.protocol.TJSONProtocol import TSimpleJSONProtocol


class ThriftServer(object):
    '''
    thrift server publish the api for server start
    '''
    __protocol_type = {
                        "tbinary": TBinaryProtocol.TBinaryProtocolFactory,
                        "taccel": TBinaryProtocol.TBinaryProtocolAcceleratedFactory,
                        "tcompact": TCompactProtocol.TCompactProtocolFactory,
                        "tjson": TJSONProtocol.TJSONProtocolFactory,
                        }
    __server_type = {
                     'simple': 'TSimpleServer',
                     'thread': 'TThreadedServer',
                     'threadpool': 'TThreadPoolServer',
                     'fork': 'TForkingServer'
                     }

    def __init__(self, processor, ip="127.0.0.1", port="9000", transport="tsocket",\
                  protocol="tbinary", server_type="thread"):
        '''
        Constructor
        '''
        self._processor = processor
        self._ip = ip
        self._port = int(port)
        self._transport = transport
        self._protocol = protocol
        self._server_type = server_type
        self.__initialize()

    def __initialize(self):
        pfactory = ThriftServer.__protocol_type[self._protocol]()
        if self._server_type == 'http':
            self._server = THttpServer.THttpServer(\
            self._processor, (self._ip, self._port), pfactory)
            return
        transport = TSocket.TServerSocket(self._ip, self._port)
        if self._transport == 'zlib':
            transport = TZlibTransport.TZlibTransport(transport)
            tfactory = TZlibTransport.TZlibTransportFactory()
        else:
            tfactory = TTransport.TBufferedTransportFactory()
        if self._server_type == 'noblock':
            self._server = TNonblockingServer.TNonblockingServer(\
            self._processor, transport, inputProtocolFactory=pfactory)
        else:
            self._server_type = ThriftServer.__server_type.get(self._server_type, 'TSimpleServer')
            ServerClass = getattr(TServer, self._server_type)
            self._server = ServerClass(self._processor, transport, tfactory, pfactory)

    def start(self):
        self._server.serve()


if __name__ == '__main__':
    #TSimpleServer, TThreadedServer,TThreadPoolServer,TForkingServer
    server_type = 'TForkingServer'
    ServerClass = getattr(TServer, server_type)
    print ServerClass
