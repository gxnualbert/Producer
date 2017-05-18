import fsp_sc_pb2ooo as pb_sc

import struct




def ClientConnected(messageSequence,client_id,service_instance_id,app_id,client_name,response_topic):
    clientconnected=pb_sc.ClientConnected()
    clientconnected.client_id=client_id
    clientconnected.service_instance_id=service_instance_id
    clientconnected.app_id=app_id
    clientconnected.client_name=client_name
    clientconnected_msg=clientconnected.SerializeToString()
    messageNumber=pb_sc.ProtoDictionary.Value('Enum2ClientConnected')
    sc_topic = response_topic
    fmt = "!BqB%dsi" % (len(sc_topic))
    #here the topic is for produce, not for consumer
    msgprefix = struct.pack(fmt, 1, messageSequence, len(sc_topic), sc_topic, messageNumber)
    # s = bytes(clientconnected_msg)
    # cpt = fsp_pb.ClientConnected()
    # cpt.ParseFromString(clientconnected_msg)
    # print cpt.client_id
    # print s
    # print clientconnected_msg
    buf = msgprefix+clientconnected_msg
    return buf

def ClientDisconnected(messageSequence,client_id,service_instance_id,response_topic):
    clientdisconnected=pb_sc.ClientDisconnected()
    clientdisconnected.client_id=client_id
    clientdisconnected.service_instance_id=service_instance_id
    clientdisconnected_msg=clientdisconnected.SerializeToString()
    messageNumber=pb_sc.ProtoDictionary.Value('Enum2ClientDisconnected')

    fmt = "!BqB%dsi" % (len(response_topic))
    # here the topic is for produce, not for consumer
    msgprefix = struct.pack(fmt, 1, messageSequence, len(response_topic), response_topic, messageNumber)
    buf = msgprefix + clientdisconnected_msg
    return buf

def CreateStream(messageSequence,app_id,stream_type,stream_property,response_topic):
    creamstream=pb_sc.CreateStream()
    creamstream.app_id=app_id
    creamstream.stream_type=stream_type
    creamstream.stream_property=stream_property
    creamstream_msg=creamstream.SerializeToString()
    createstream_Enum_Number=pb_sc.ProtoDictionary.Value('Enum2CreateStream')

    fmt="!BqB%dsi"%(len(response_topic))
    msgprefix = struct.pack(fmt, 1, messageSequence, len(response_topic), response_topic, createstream_Enum_Number)
    buf=msgprefix+creamstream_msg
    return buf
def CheckStreamPublishToken(messageSequence,stream_id,stream_public_token,response_topic):
    checkstreampublishtoken=pb_sc.CheckStreamPublishToken()
    checkstreampublishtoken.stream_id=stream_id
    checkstreampublishtoken.stream_public_token=stream_public_token
    checkstreampublishtoken_msg=checkstreampublishtoken.SerializeToString()
    checkstreampublishtoken_Enum_Number=pb_sc.ProtoDictionary.Value('Enum2CheckStreamPublishToken')

    fmt="!BqB%dsi"%(len(response_topic))
    msgprefix=struct.pack(fmt,1,messageSequence,len(response_topic),response_topic,checkstreampublishtoken_Enum_Number)

    buf=msgprefix+checkstreampublishtoken_msg
    return buf

def PublishStreamCP(messageSequence,stream_id,client_id,client_ip,response_topic):
    publishstreamcp=pb_sc.PublishStream()
    publishstreamcp.stream_id=stream_id
    publishstreamcp.client_id=client_id
    publishstreamcp.client_ip=client_ip

    publishstreamcp_msg=publishstreamcp.SerializeToString()
    publishstreamcp_Enum_Num=pb_sc.ProtoDictionary.Value('Enum2PublishStream')

    fmt = "!BqB%dsi" % (len(response_topic))
    msgprefix=struct.pack(fmt,1,messageSequence,len(response_topic),response_topic,publishstreamcp_Enum_Num)

    buf=msgprefix+publishstreamcp_msg
    return buf
def GetStreamServersCP(messageSequence,stream_id,client_id,client_ip,exception_servers,response_topic):
    getstreamserverscp=pb_sc.GetStreamServers()
    getstreamserverscp.stream_id=stream_id
    getstreamserverscp.client_id=client_id
    getstreamserverscp.client_ip=client_ip
    for i in exception_servers:
        getstreamserverscp.exception_servers.append(i)

    getstreamserverscp_msg=getstreamserverscp.SerializeToString()
    getstreamserverscp_Enum_Num=pb_sc.ProtoDictionary.Value('Enum2GetStreamServers')

    fmt = "!BqB%dsi" % (len(response_topic))
    msgprefix=struct.pack(fmt,1,messageSequence,len(response_topic),response_topic,getstreamserverscp_Enum_Num)

    buf=msgprefix+getstreamserverscp_msg
    return buf

def ChannelConnected(messageSequence,client_id,service_instance_id,stream_id,direction,response_topic):
    channerlconnected=pb_sc.ChannelConnected()
    channerlconnected.client_id=client_id
    channerlconnected.service_instance_id=service_instance_id
    channerlconnected.stream_id=stream_id
    channerlconnected.direction=direction
    channerlconnected_msg=channerlconnected.SerializeToString()
    channerlconnected_Enum_Num=pb_sc.ProtoDictionary.Value('Enum2ChannelConnected')

    fmt = "!BqB%dsi" % (len(response_topic))
    msgprefix=struct.pack(fmt, 1, messageSequence, len(response_topic), response_topic, channerlconnected_Enum_Num)

    buf=msgprefix+channerlconnected_msg

    return buf


