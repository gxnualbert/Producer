from kafka import KafkaProducer
import login as pbbuf
import time
import threading
import redis
import fsp_common_pb2oooo as pb_common

class Produce(object):
    kafkaCluster='192.168.7.63:9092,192.168.7.64:9092,192.168.7.65:9092'
    producer = KafkaProducer(bootstrap_servers=kafkaCluster)
    r=redis.StrictRedis(host="192.168.7.107",port='6379',db=0)

    def __init__(self):
        pass
    def ClientConnected(self,messageSequence,topicName,client_id,service_instance_id,app_id,client_name,response_topic):
      
        cpbuf = pbbuf.ClientConnected(messageSequence,client_id,service_instance_id,app_id,client_name,response_topic)
        Produce.producer.send(topicName, cpbuf)
        print "send data successfully"
        
    def ClientDisconnected(self,messageSequence,topic_name,client_id,service_instance_id,response_topic):
        '''
        :param messageSequence:
        :param topicName:
        :param client_id:
        :param service_instance_id:
        :param response_topic:
        :return:
        '''
        client_disconnected_buf=pbbuf.ClientDisconnected(messageSequence,client_id,service_instance_id,response_topic)
        Produce.producer.send(topic_name,client_disconnected_buf)
    def CreateStream(self,messageSequence,topic_name,app_id,stream_type,stream_property,response_topic):
        createstream_buf=pbbuf.CreateStream(messageSequence,app_id,stream_type,stream_property,response_topic)
        Produce.producer.send(topic_name,createstream_buf)

    def RFCreateStream(self,messageSequence,topic_name,app_id,response_topic):

        stream_type=pb_common.StreamType.Value("EnumVideoStream")
        stream_property=pb_common.StreamProperty.Value("EnumReliable")
        createstream_buf=pbbuf.CreateStream(messageSequence,app_id,stream_type,stream_property,response_topic)
        Produce.producer.send(topic_name,createstream_buf)

    def CheckStreamPublishToken(self,messageSequence,topic_name,stream_id,stream_public_token,response_topic):
        checkstreampublishtoken_buf=pbbuf.CheckStreamPublishToken(messageSequence,stream_id,stream_public_token,response_topic)
        Produce.producer.send(topic_name,checkstreampublishtoken_buf)
    def PublishStreamCP(self,messageSequence,topic_name,stream_id,client_id,client_ip,response_topic):
        publishstreamcp_buf=pbbuf.PublishStreamCP(messageSequence,stream_id,client_id,client_ip,response_topic)
        Produce.producer.send(topic_name,publishstreamcp_buf)
    def GetStreamServersCP(self,messageSequence,topic_name,stream_id,client_id,client_ip,exception_servers,response_topic):
        getstreamserverscp_buf=pbbuf.GetStreamServersCP(messageSequence,stream_id,client_id,client_ip,exception_servers,response_topic)
        Produce.producer.send(topic_name,getstreamserverscp_buf)
    def ChannelConnected(self,messageSequence,topic_name,client_id,service_instance_id,stream_id,direction,response_topic):
        print type(direction),direction
        if direction=="Sending":
            channerlconnected_buf=pbbuf.ChannelConnected(messageSequence,client_id,service_instance_id,stream_id,pb_common.DataDirection.Value('Sending'),response_topic)
        elif direction=="Receiving":
            channerlconnected_buf=pbbuf.ChannelConnected(messageSequence,client_id,service_instance_id,stream_id,pb_common.DataDirection.Value('Receiving'),response_topic)
        else:
            print "Please set the DataDirection!!"
        Produce.producer.send(topic_name,channerlconnected_buf)
    
    def QueryDB_clientConnected(self,messageSequence,topicName,client_id,service_instance_id,app_id,client_name,response_topic):

        self.ClientConnected(messageSequence, topicName, client_id, service_instance_id, app_id, client_name, response_topic)
        query_result = Produce.r.get('client_proxy:'+client_id)

        if service_instance_id in query_result:
            print "find cp service instance id: %s in db successfully"%service_instance_id
            return True
        else:
            print "fail to find cp service instance id: %s in db "%service_instance_id
	    return False
    def ReadFile(self):
        f = open("/root/PycharmProjects/consumer/a.txt", 'r')
        rep_str = f.readlines()
        print rep_str[0]
        rsp=rep_str[0]
        f.close()

        if "created stream" in rsp:
            stream_id=rsp.split("created stream")[1]
            stream_id=stream_id.strip(" ")
            print stream_id
            app_id=Produce.r.hget(stream_id,"app_id")
            print app_id
            return app_id
    def ReadStreamIDForChannelConnected(self):
        f = open("/root/PycharmProjects/consumer/a.txt", 'r')
        rsp_str = f.readlines()
        rsp_str=rsp_str[0]
        stream_id=rsp_str.split("stream")[1]
        stream_id = stream_id.strip(" ")
        f.close()
        return stream_id
    def CheckChannelConnected(self,clientid,streamid):
        f = open("/root/PycharmProjects/consumer/a.txt", 'r')
        rsp_str = f.readlines()
        rsp_str = rsp_str[0]
        if clientid in rsp_str and streamid in rsp_str:
            print rsp_str
            return True
        else:
            print "Fail to find the client id and stream id in rsp msg!"
            return False


a=Produce()
a.ClientConnected(1,"sc_instance103","albert","ssxxxxx","1","albertclient","cp_test1")
