package com.ociweb.pronghorn.network.mqtt;

import com.ociweb.pronghorn.network.schema.MQTTConnectionInSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;

public class PacketIdReleaseManager {
    private int firstConsumedPacketId;
    private int lastConsumedPacketId;

    public PacketIdReleaseManager() {
        this.firstConsumedPacketId = -1;
        this.lastConsumedPacketId = -1;
    }

    private static void releasePacketId(PacketIdReleaseManager instance, MQTTConnectionStage connectionStage, int packetId) {
        
        //most common expected case first
        if (packetId == instance.lastConsumedPacketId + 1) {
            instance.lastConsumedPacketId = packetId;
        } else if (packetId == instance.firstConsumedPacketId - 1) {
            instance.firstConsumedPacketId = packetId;
        } else {
            //push the old range back to idGen
            int result = (instance.firstConsumedPacketId<<16) | (instance.lastConsumedPacketId+1);
            //before parse of incoming message we have already checked that there is room on the outgoing queue
            Pipe.addMsgIdx(connectionStage.idGenOut, connectionStage.getIdMessageIdx);   
            Pipe.addIntValue(result, connectionStage.idGenOut);
            Pipe.publishWrites(connectionStage.idGenOut);  
            Pipe.confirmLowLevelWrite(connectionStage.idGenOut, connectionStage.genIdMessageSize);
            //now use packetIda as the beginning of a new group
            instance.firstConsumedPacketId = instance.lastConsumedPacketId = packetId;
        }
    }

    public static int releaseMessage(PacketIdReleaseManager instance, MQTTConnectionStage connectionStage, int packetId, int originalQoS) {	    
    
            Pipe.replayUnReleased(connectionStage.apiIn);
            
            int result = 0; //Must return the originalQoS only when it is found and cleared.
            boolean foundMustNotReleasePoint = false;
            boolean replaying = true;
            while (replaying && PipeReader.tryReadFragment(connectionStage.apiIn)) {
                //always checks one more, the current one which is not technically part of the replay.
                replaying = Pipe.isReplaying(connectionStage.apiIn);	            
                int msgIdx = PipeReader.getMsgIdx(connectionStage.apiIn);
                
                //based on type 
                if (originalQoS < 3 && MQTTConnectionInSchema.MSG_PUBLISH_1 == msgIdx) {    
                    int msgPacketId = PipeReader.readInt(connectionStage.apiIn, MQTTConnectionInSchema.MSG_PUBLISH_1_FIELD_PACKETID_200);
                
                    if (!findMatchingPacketId(instance, connectionStage, packetId, originalQoS, msgPacketId)) {	       
                        int qos = PipeReader.readInt(connectionStage.apiIn, MQTTConnectionInSchema.MSG_PUBLISH_1_FIELD_QOS_100);
                        foundMustNotReleasePoint |= (qos>0);//skip over and release all qos zeros this only stops on un-confirmed 1 and 2	                
                    } else {
                        PipeReader.releaseReadLock(connectionStage.apiIn);
                        result = originalQoS;
                        //we found it so stop looking now
                        if (!foundMustNotReleasePoint) {
                            //System.out.println("A Released up to  "+RingBuffer.getWorkingTailPosition(connectionStage.apiIn));
                            Pipe.releaseBatchedReadReleasesUpToThisPosition(connectionStage.apiIn);
                        }
                        break;
                    }
                    
                } else if (originalQoS==3 && MQTTConnectionInSchema.MSG_PUBREL_9 == msgIdx) {                    
                    result = releasePubRelMessage(instance, connectionStage, packetId, originalQoS);
                } else {
                    System.err.println("unkown "+msgIdx);
                }	            	            
                PipeReader.releaseReadLock(connectionStage.apiIn);
                if (!foundMustNotReleasePoint) {
                    System.out.println("B Released up to  "+Pipe.getWorkingTailPosition(connectionStage.apiIn)+"  "+connectionStage.apiIn+"  "+connectionStage.apiIn.sizeOfSlabRing+" "+connectionStage.apiIn.sizeOfBlobRing);
                    Pipe.releaseBatchedReadReleasesUpToThisPosition(connectionStage.apiIn);
                }
            }
            Pipe.cancelReplay(connectionStage.apiIn);
            return result;
    }

    private static int releasePubRelMessage(PacketIdReleaseManager instance, MQTTConnectionStage connectionStage,
            int packetId, int origValue) {
        int msgPacketId = PipeReader.readInt(connectionStage.apiIn, MQTTConnectionInSchema.MSG_PUBREL_9_FIELD_PACKETID_200);
        if (packetId == msgPacketId) {        
            System.err.println("release for packet pubRel "+packetId);
            
            //we found the pubRel now clear it by setting the packet id negative
            PipeReader.readIntSecure(connectionStage.apiIn, MQTTConnectionInSchema.MSG_PUBREL_9_FIELD_PACKETID_200,-msgPacketId);
            PacketIdReleaseManager.releasePacketId(instance, connectionStage, packetId);//This is the end of the QoS2 publish
            return origValue;
        }
        return 0;
    }

    public static boolean findMatchingPacketId(PacketIdReleaseManager instance, MQTTConnectionStage connectionStage,
            int packetId, int originalQoS, int msgPacketId) {
        boolean foundId = packetId == msgPacketId;
        if (foundId) {
            //we found it, now clear the QoS and confirm that it was valid
            int qos = PipeReader.readIntSecure(connectionStage.apiIn, MQTTConnectionInSchema.MSG_PUBLISH_1_FIELD_QOS_100,-originalQoS);
            if (1==qos) {
                PacketIdReleaseManager.releasePacketId(instance, connectionStage, packetId);//This is the end of the QoS1 publish	                        
            } else if (qos<=0) {
                //this conditional is checked last because it is not expected to be frequent
                MQTTConnectionStage.log.warn("reduntant ack");
            }
        }
        return foundId;
    }
}