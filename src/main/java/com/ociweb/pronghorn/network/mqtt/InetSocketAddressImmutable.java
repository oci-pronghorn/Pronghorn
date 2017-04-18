package com.ociweb.pronghorn.network.mqtt;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

public class InetSocketAddressImmutable extends InetSocketAddress {

    private String value; 
            
    public InetSocketAddressImmutable(String hostname, int port) {
        super(addrWrap(hostname), checkPort(port));    
    }
    
    private static InetAddress addrWrap(String hostname) {
        if (hostname == null)
            throw new IllegalArgumentException("hostname can't be null");
        try {
            InetAddress result = InetAddress.getByName(hostname);
            
            
            
            //this ToString is called and is TOO expensive!!
            //result.toString()
            
            return result;
        } catch (UnknownHostException e) {
           throw new IllegalArgumentException(e);
        }        
    }
    
    private static int checkPort(int port) {
        if (port < 0 || port > 0xFFFF)
            throw new IllegalArgumentException("port out of range:" + port);
        return port;
    }
    
    public void reset() {
        value = null;
    }
    
    @Override
    public String toString() {
        if (null == value) {
            value = super.toString();
        }
        return value;
    }
    
    
}
