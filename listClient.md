package com.att.cloud.so.nsapi.handlers.clientsubnet;

import java.sql.ResultSet;
import java.util.List;

import javax.servlet.http.HttpServletResponse;

import com.att.cloud.so.cloudapi.common.RecordType;
import com.att.cloud.so.cloudapi.messages.SeverityType;
import com.att.cloud.so.handlers.JAXPipelineHandler; 
import com.att.cloud.so.interfaces.pipeline.PipelineException;
import com.att.cloud.so.interfaces.pipeline.PipelineMessage;
import com.att.cloud.so.interfaces.pipeline.PipelineMessageDictionary; 
import com.att.cloud.so.nsapi.messages.AttNSApiTypes;  
import com.att.cloud.so.nsapi.messages.ClientSubnetListType;
import com.att.cloud.so.nsapi.messages.ClientSubnetType;
import com.att.cloud.so.nsapi.messages.LinkType; 
import com.att.cloud.so.nsapi.messages.RirType;
import com.att.cloud.so.nsapi.messages.SubnetStateType;
import com.att.cloud.so.utils.DBAccess; 
import com.att.cloud.so.utils.LogEngine;
import com.att.cloud.so.utils.Tools;  
import com.att.cloud.so.utils.XMLUtils;
import com.att.cloud.so.nsapi.messages.NsStatusType;

public class ListClientSubnetHandler extends JAXPipelineHandler{
	 	 
	private static final long serialVersionUID = 1L;

	private static String RETRIEVE_NSAPI_CLIENT_SUBNET = "SELECT IDENTIFIER, SUBNET, SVCINSTANCEID,"
            + " NAME, TYPE, ASN, RIR, STATE, STATUS, CREATED, MODIFIED "
            + " FROM NSAPI_CLIENT_SUBNET ";
	
	private static String RETRIEVE_NSAPI_CLIENT_SUBNET_SVCINSTANCEID = "SELECT IDENTIFIER, SUBNET, SVCINSTANCEID,"
            + " NAME, TYPE, ASN, RIR, STATE, STATUS, CREATED, MODIFIED "
            + " FROM NSAPI_CLIENT_SUBNET WHERE SVCINSTANCEID = ?  ";	
	    
	public void execute(PipelineMessage pmsg) throws PipelineException {
			
		try {  
			LogEngine.debug("<---  Entering ListClientSubnetHandler  --->");	
			Tools.log("<---  Entering ListClientSubnetHandler  --->");	
			   
			ClientSubnetListType clientSubnetListType = new ClientSubnetListType(); 
			
			validateSvcInstanceId(pmsg);
			
			processRecordType(clientSubnetListType, pmsg);	 

			pmsg.addAttribute(PipelineMessageDictionary.NSAPI_CLIENTSUBNET_LIST, clientSubnetListType);
			 			
			pmsg.getResponse().setResponseCode(HttpServletResponse.SC_OK);
				
			LogEngine.debug("<---  Exiting ListRoutingDomainHandler  --->");
			Tools.log("<---  Exiting ListRoutingDomainHandler  --->");
					
		} catch (Exception e) {
			LogEngine.logException(e); 
			pmsg.getResponse().addErrmsg(e.getMessage(), HttpServletResponse.SC_INTERNAL_SERVER_ERROR , SeverityType.FATAL,  e.getClass().getName());	
		} 
	}  
		
	private static String VALIDATE_SVCINSTANCE_ID = "SELECT COUNT(*) CNT FROM NSAPI_CLIENT_SUBNET WHERE SVCINSTANCEID = ? ";
	protected void validateSvcInstanceId(PipelineMessage pmsg) throws Exception {
		  
		String uri = pmsg.getUriAsStr();		
		String svcInstanceId = null;		
	
		DBAccess dba = null;
		ResultSet rs = null;
   		
		try {
			
			 if (Tools.isNotEmpty(pmsg.getAttributeAsStr(PipelineMessageDictionary.NSAPI_SVC_INSTANCE_ID))) {
				//If svcInstanceId passed in the header then use that
				svcInstanceId = pmsg.getAttributeAsStr(PipelineMessageDictionary.NSAPI_SVC_INSTANCE_ID);
				}			
			if (uri.toUpperCase().indexOf("SVCINSTANCEID=") > -1 ) {  
				svcInstanceId = uri.substring(uri.toUpperCase().indexOf("SVCINSTANCEID=") + ("SVCINSTANCEID=").length(), uri.length()) ;
				if (svcInstanceId.indexOf("&") > -1) {
					svcInstanceId = svcInstanceId.substring(0, svcInstanceId.indexOf("&")) ;
				}			
				pmsg.addAttribute(PipelineMessageDictionary.NSAPI_SVC_INSTANCE_ID, svcInstanceId);				
			}
									
										
			LogEngine.debug("SvcInstance Id : " + svcInstanceId);
			Tools.log("SvcInstance Id : " + svcInstanceId);
	 	    if (Tools.isNotEmpty(svcInstanceId)) {
	 	    	dba = new DBAccess();
	 	    	dba.prepareStatement(VALIDATE_SVCINSTANCE_ID);
	 	    	dba.setString(1, svcInstanceId);
	 	    	rs = dba.executeQuery(); 
	 	    	if( null != rs && rs.next()){
	 	    		int record = rs.getInt("CNT");
	 	    		if(record <= 0){ 
	 	    			pmsg.getResponse().setResponseCode(HttpServletResponse.SC_NOT_FOUND); 
	 	    			LogEngine.debug("The clientsubnet with svcInstanceId: " + svcInstanceId + " doesn't exist.");
						Tools.log("The clientsubnet with svcInstanceId: " + svcInstanceId + " doesn't exist.");
						throw new Exception("The clientsubnet with svcInstanceId: " + svcInstanceId + " doesn't exist.");
	 	    		}
	 	    	} else {
	 	    		pmsg.getResponse().setResponseCode(HttpServletResponse.SC_NOT_FOUND); 
	 	    		throw new Exception("There is no record for svcInstanceId:" + svcInstanceId);
	 	    	} 
	 	    }	
		} catch (Exception e) {
			throw e;
		}finally{
			if( Tools.isNotEmpty(dba)){
				dba.releasePStmt();
				dba.close(rs);
			}
		}  
	}  
				 
	protected void processRecordType(ClientSubnetListType clientSubnetListType, PipelineMessage pmsg) throws Exception{
			  	
		String uri = pmsg.getUriAsStr();  
		uri = uri.indexOf("?") > 0? uri.substring(0, uri.indexOf("?")) : uri;
		LogEngine.debug("URI : "+uri);
		Tools.log("URI : "+uri);
		
		if(pmsg.getUriAsStr().indexOf("recordType")==-1) {
			LogEngine.debug("List Client Subnet as Link - Default");
			Tools.log("List Client Subnet as Link - Default");
			getClientSubnetAsLink(clientSubnetListType, pmsg, uri);
		}else {  
			if (RecordType.fromString(pmsg.getAttributeAsStr("recordType")).isOBJECT()){
				LogEngine.debug("List Client Subnet as Object");
				Tools.log("List Client Subnet as Object");
				getClientSubnetInfoAsObject(clientSubnetListType, pmsg, uri); 
			}else{
				LogEngine.debug("List Client Subnet as Link");
				Tools.log("List Client Subnet as Link");
				getClientSubnetAsLink(clientSubnetListType, pmsg, uri);
			}
		}  
	}
	
	protected void getClientSubnetAsLink(ClientSubnetListType clientSubnetListType, PipelineMessage pmsg, String uri) throws Exception{
		
		List<LinkType> clientSubnetLinkTypeList = clientSubnetListType.getLink();
		String svcinstance = pmsg.getAttributeAsStr(PipelineMessageDictionary.NSAPI_SVC_INSTANCE_ID);
		
		DBAccess dba = null;
		ResultSet rs = null;
			
		try {
			
			dba = new DBAccess(); 
			if (Tools.isEmpty(svcinstance)) {  
				LogEngine.debug("SQL: " +  RETRIEVE_NSAPI_CLIENT_SUBNET);
				Tools.log("SQL: " +  RETRIEVE_NSAPI_CLIENT_SUBNET);
				dba.prepareStatement(RETRIEVE_NSAPI_CLIENT_SUBNET);	 
				//dba.setString(1, identifier); 
			} else { 
				LogEngine.debug("SQL: " +  RETRIEVE_NSAPI_CLIENT_SUBNET_SVCINSTANCEID);
				Tools.log("SQL: " +  RETRIEVE_NSAPI_CLIENT_SUBNET_SVCINSTANCEID);
				dba.prepareStatement(RETRIEVE_NSAPI_CLIENT_SUBNET_SVCINSTANCEID);	 				
				dba.setString(1, svcinstance); 
			}  
		 
			rs = dba.executeQuery();			
			LinkType link = null;	
			while (rs != null && rs.next()) {
				ClientSubnetType clientSubnetType = new ClientSubnetType();
				link = new LinkType();
				clientSubnetType.setIdentifier(rs.getString("IDENTIFIER"));  
				link.setAction("LIST");
				link.setMethod("GET");
				link.setId(rs.getString("IDENTIFIER")); 
				link.setName(rs.getString("NAME"));
				link.setHref(uri + "/" + rs.getString("IDENTIFIER"));  								
				link.setType(AttNSApiTypes.APPLICATION_VND_ATT_SYNAPTIC_ATTNSAPI_CLIENT_SUBNET_LIST_XML.value()); 
				getRnatAsLink(clientSubnetType.getLink(), clientSubnetType.getIdentifier() ,pmsg);				
				clientSubnetLinkTypeList.add(link);
			}
			
		} catch (Exception e) {
			throw e;
		} finally {
			if( Tools.isNotEmpty(dba)){
				dba.releasePStmt();
				dba.close(rs);
			}
		} 
	}  
	
	 
	protected void getClientSubnetInfoAsObject(ClientSubnetListType clientSubnetListType, PipelineMessage pmsg, String uri) throws Exception{
		
		List<ClientSubnetType> clientSubnetTypeList = clientSubnetListType.getClientSubnet();
		String svcinstance = pmsg.getAttributeAsStr(PipelineMessageDictionary.NSAPI_SVC_INSTANCE_ID);
		DBAccess dba = null;
		ResultSet rs = null;

		try { 
			dba = new DBAccess(); 
			if (Tools.isEmpty(svcinstance)) { 
				LogEngine.debug("SQL RETRIEVE_NSAPI_CLIENT_SUBNET: " +  RETRIEVE_NSAPI_CLIENT_SUBNET);
				Tools.log("SQL RETRIEVE_NSAPI_CLIENT_SUBNET: " +  RETRIEVE_NSAPI_CLIENT_SUBNET);
				dba.prepareStatement(RETRIEVE_NSAPI_CLIENT_SUBNET);	
				//dba.setString(1, identifier);
			} else { 
				LogEngine.debug("SQL RETRIEVE_NSAPI_CLIENT_SUBNET_SVCINSTANCEID: " +  RETRIEVE_NSAPI_CLIENT_SUBNET_SVCINSTANCEID);
				Tools.log("SQL RETRIEVE_NSAPI_CLIENT_SUBNET_SVCINSTANCEID: " +  RETRIEVE_NSAPI_CLIENT_SUBNET_SVCINSTANCEID);
				dba.prepareStatement(RETRIEVE_NSAPI_CLIENT_SUBNET_SVCINSTANCEID);	 
				//dba.setString(1, identifier);
				dba.setString(1, svcinstance);
			} 
			rs = dba.executeQuery();
			while (rs != null && rs.next()) {
				ClientSubnetType clientSubnetType = new ClientSubnetType(); 
				clientSubnetType.setIdentifier(rs.getString("IDENTIFIER")); 
				clientSubnetType.setName(rs.getString("NAME"));
            	clientSubnetType.setServiceInstanceId(rs.getString("SVCINSTANCEID"));
            	clientSubnetType.setSubnet(rs.getString("SUBNET"));
            	clientSubnetType.setAsn(rs.getString("ASN"));
            	if(Tools.isNotEmpty(rs.getString("RIR"))){
            		clientSubnetType.setRIR(RirType.fromValue(rs.getString("RIR")));	
            	}
            	clientSubnetType.setState(SubnetStateType.fromValue(rs.getString("STATE")));	            	         
            	clientSubnetType.setSubnetType(rs.getNString("TYPE"));
                clientSubnetType.setStatus(NsStatusType.fromValue(rs.getString("STATUS")));
                clientSubnetType.setLastModified(Tools.toXMLDate(rs.getDate("MODIFIED")));	 
                clientSubnetType.setHref(uri + "/" + rs.getString("IDENTIFIER"));  								
                clientSubnetType.setType(AttNSApiTypes.APPLICATION_VND_ATT_SYNAPTIC_ATTNSAPI_CLIENT_SUBNET_LIST_XML.value()); 
                getRnatAsLink(clientSubnetType.getLink(),clientSubnetType.getIdentifier(), pmsg);
				clientSubnetTypeList.add(clientSubnetType);
			} 
		} catch (Exception e) {
			LogEngine.logException(e);
			throw e;
		} finally {
			if( Tools.isNotEmpty(dba)){
				dba.releasePStmt();
				dba.close(rs);
			}
		}  
	}	
	private static String GET_RNAT_LINK =
            "SELECT a.IDENTIFIER, a.SUBNET, a.STATE, a.STATUS, b.IDENTIFIER as RNATID, b.VLANID"                
                + " FROM NSAPI_CLIENT_SUBNET a, NSAPI_RVLAN b WHERE a.IDENTIFIER = b.TRAN_SRC_SUBNETID AND a.IDENTIFIER = ?"; 
    
    public void getRnatAsLink(List<LinkType> links ,String id, PipelineMessage pmsg) throws Exception{	
    String uri = pmsg.getUriAsStr(); 
	uri = uri.indexOf("?") > 0? uri.substring(0, uri.indexOf("?")) : uri; 		
	DBAccess dba = null;
	ResultSet rs = null;
	try {
		dba = new DBAccess();
		dba.prepareStatement(GET_RNAT_LINK);
		dba.setString(1, id);
		rs = dba.executeQuery();		
		LinkType lt = null;
		while (rs != null && rs.next()) {
			lt = new LinkType(); 			
			lt.setId(rs.getString("IDENTIFIER")); 
			lt.setHref(uri + "/" + rs.getString("RNATID") + "/" + rs.getString("VLANID"));  								
			lt.setType(AttNSApiTypes.APPLICATION_VND_ATT_SYNAPTIC_ATTNSAPI_RNAT_XML.value());
			links.add(lt);
		}
	} catch (Exception e) {
		throw e;
	}finally{
		if( Tools.isNotEmpty(dba)){
			dba.releasePStmt();
			dba.close(rs);
		}
	} 
}
}  		 
