from kafka import KafkaProducer
import login as pbbuf
import time
import threading

# kafkaCluster='192.168.7.60:9092,192.168.7.61:9092,192.168.7.62:9092'
kafkaCluster='192.168.7.63:9092,192.168.7.64:9092,192.168.7.65:9092'
producer=KafkaProducer(bootstrap_servers=kafkaCluster)

# cpbuf=pbbuf.ClientConnected(87,"ablert_test")
# producer.send('ablert_test',cpbuf)
# def test(msg):
#     # here , the topic is for consumer
#     for i in range(401, 402444):
#
#         # cpbuf = pbbuf.ClientConnected(i,'client_id',"cp_1","1","albert_test_client","cp_test1")
#         cpbuf=i
#         producer.send('albert', bytes(i)+bytes(msg))
#         time.sleep(1)
#         # cpt = fsp_pb.ClientConnected()
#         # cpt.ParseFromString(cpbuf[5])
#         # print cpt.client_id
#         # print cpt.app_id
#         # print cpt.client_name
#
#         print i

###################-----------------test function start----------------------------


# threads=[]
# for i in range(100):
#     a=threading.Thread(target=test, args=('test'+str(i),))
#     threads.append(a)
#
# if __name__=='__main__':
#     print "main"
#     for t in threads:
#         t.setDaemon(True)
#         t.start()
#
#     t.join()

######################---------------test function end-------------------------------






def ClientConnected(messageSequence,topicName,client_id,service_instance_id,app_id,client_name,response_topic):
    '''

    :param messageSequence:
    :param topicName: The topic which SC listening
    :param client_id:
    :param service_instance_id:
    :param app_id:
    :param client_name:
    :return:
    '''
    cpbuf = pbbuf.ClientConnected(messageSequence,client_id,service_instance_id,app_id,client_name,response_topic)
    producer.send(topicName, cpbuf)
def ClientDisconnected(messageSequence,topic_name,client_id,service_instance_id,response_topic):
    '''

    :param messageSequence:
    :param topicName:
    :param client_id:
    :param service_instance_id:
    :param response_topic:
    :return:
    '''

    client_disconnected_buf=pbbuf.ClientDisconnected(messageSequence,client_id,service_instance_id,response_topic)
    producer.send(topic_name,client_disconnected_buf)
def CreateStream(messageSequence,topic_name,app_id,stream_type,stream_property,response_topic):
    createstream_buf=pbbuf.CreateStream(messageSequence,app_id,stream_type,stream_property,response_topic)
    producer.send(topic_name,createstream_buf)
def CheckStreamPublishToken(messageSequence,topic_name,stream_id,stream_public_token,response_topic):
    checkstreampublishtoken_buf=pbbuf.CheckStreamPublishToken(messageSequence,stream_id,stream_public_token,response_topic)
    producer.send(topic_name,checkstreampublishtoken_buf)
def PublishStreamCP(messageSequence,topic_name,stream_id,client_id,client_ip,response_topic):
    publishstreamcp_buf=pbbuf.PublishStreamCP(messageSequence,stream_id,client_id,client_ip,response_topic)
    producer.send(topic_name,publishstreamcp_buf)
def GetStreamServersCP(messageSequence,topic_name,stream_id,client_id,client_ip,exception_servers,response_topic):
    getstreamserverscp_buf=pbbuf.GetStreamServersCP(messageSequence,stream_id,client_id,client_ip,exception_servers,response_topic)
    producer.send(topic_name,getstreamserverscp_buf)
def ChannelConnected(messageSequence,topic_name,client_id,service_instance_id,stream_id,direction,response_topic):
    channerlconnected_buf=pbbuf.ChannelConnected(messageSequence,client_id,service_instance_id,stream_id,direction,response_topic)
    producer.send(topic_name,channerlconnected_buf)










# ClientConnected(3,'client_id',"cp_1","1","albert_test_client","cp_test1")
ClientConnected(1,'sc_instance103rttt','client_id7d8','cp_servive2','app_id_2','albert_client',"cp_test1")

# ClientDisconnected(5,'sc_test_instance174',"1;timb15d47e99831ee63e3f47cf3d4478e9a","cp_1","cp_test1")
# CreateStream(2,"sc_test_instance174","1",fsp_pb.StreamType.Value("EnumVideoStream"),fsp_pb.StreamProperty.Value("EnumReliable"),"cp_test1")
# CheckStreamPublishToken(1,"sc_test_instance174","40d2f989-347e-48a6-87a8-bed9e2169c40","WT8W77+977+92ZLvv71PQ++/ve+/vWbvv71bSO+/vSXvv73vv70GPE0OWO+/vW0eWE/OpxMO77+9\nSyrvv73vv73vv73vv70D77+9ZCPvv71977+9ehrvv71a77+977+9BiF377+9GO+/ve+/vR3vv71P\nBe+/vTcMEO+/vX8077+9ACVBEO+/ve+/ve+/vS9qLz3vv73vv70O77+977+9Ae+/ve+/vTwvb0Tv\nv73vv73vv70Z77+9AO+/ve+/ve+/ve+/ve+/ve+/ve+/vV1kae+/ve+/ve+/ve+/vd2v77+977+9\n77+977+9VSTvv71nSA==","cp_test1")

# PublishStreamCP(1,"sc_test_instance174","732012bb-50d0-46a7-869c-e39d7e3b0453","1;albert_client_id","192.168.6.263","cp_test1")
# exception_servers=["192.168.7.109","192.168.7.144"]
# GetStreamServersCP(1,"sc_test_instance174","732012bb-50d0-46a7-869c-e39d7e3b0453","1;albert_client_id","192.168.6.263",exception_servers,"cp_test1")
# ChannelConnected(1,"sc_test_instance174","1;albert_client_id","stream_instanttttt_3","732012bb-50d0-46a7-869c-e39d7e3b0453",fsp_pb.DataDirection.Value('Receiving'),"cp_test1")

# test()
