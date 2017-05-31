package com.hortonworks.nifi.processors;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.stream.io.StreamUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.hortonworks.nifi.controller.api.LivySessionService;

@SideEffectFree
@Tags({"Spark","Livy","HTTP"})
@CapabilityDescription("Sends events to Apache Druid for Indexing")
public class ExecuteSparkInteractive extends AbstractProcessor {
	private List<PropertyDescriptor> properties;
	private Set<Relationship> relationships;
	//private FlowFile flowFile;

    public static final PropertyDescriptor LIVY_CONTROLLER_SERVICE = new PropertyDescriptor.Builder()
            .name("livy_controller_service")
            .description("Livy Controller Service")
            .required(true)
            .identifiesControllerService(LivySessionService.class)
            .build();
	
	public static final Relationship REL_SUCCESS = new Relationship.Builder()
	        .name("SUCCESS")
	        .description("Succes relationship")
	        .build();
	
	/*public static final Relationship REL_ORIGINAL = new Relationship.Builder()
	        .name("ORIGINAL")
	        .description("Original flowfile relationship")
	        .build();*/
	
    public static final Relationship REL_FAIL = new Relationship.Builder()
            .name("FAIL")
            .description("FlowFiles are routed to this relationship when they cannot be parsed")
            .build();
    
	public void init(final ProcessorInitializationContext context){
	    List<PropertyDescriptor> properties = new ArrayList<>();
	    properties.add(LIVY_CONTROLLER_SERVICE);
	    this.properties = Collections.unmodifiableList(properties);
		
	    Set<Relationship> relationships = new HashSet<Relationship>();
	    relationships.add(REL_SUCCESS);
	    //relationships.add(REL_ORIGINAL);
	    relationships.add(REL_FAIL);
	    this.relationships = Collections.unmodifiableSet(relationships);
	}

	@Override
	public Set<Relationship> getRelationships(){
	    return relationships;
	}
	
	@Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }
	
	@Override
	public void onTrigger(ProcessContext context, final ProcessSession session) throws ProcessException {
		//ProvenanceReporter provRep = session.getProvenanceReporter();
		
		Map<String, String> livyController = (Map<String,String>) context.getProperty(LIVY_CONTROLLER_SERVICE).asControllerService(LivySessionService.class).getSession();
		
        FlowFile flowFile = session.get();
        if (flowFile == null) {
        	//session.remove(flowFile);
        	return;
        }else{
        	final byte[] buffer = new byte[(int) flowFile.getSize()];
        	session.read(flowFile, new InputStreamCallback() {
        		@Override
        		public void process(final InputStream in) throws IOException {
        			StreamUtils.fillBuffer(in, buffer);
        		}
        	});
        
        	String sessionId = livyController.get("sessionId").toString();
        	String livyUrl = livyController.get("livyUrl").toString();
        	String payload = "{\"code\":\""+flowFile.getAttribute("code")+"\"}";
        	JSONObject result = submitAndHandleJob(livyUrl,sessionId,payload);
        	getLogger().debug("********** ExecuteSparkInteractive Resutl of Job Submit: " + result);
        	if(result==null){
        		session.transfer(flowFile, REL_FAIL);
        	}else{
        		flowFile = session.write(flowFile, new OutputStreamCallback() {
                	public void process(OutputStream out) throws IOException {
                		out.write(result.toString().getBytes());
                	}
        		});
        		//flowFile = session.putAllAttributes(flowFile, (Map<String, String>) new ArrayList());\
        		session.transfer(flowFile, REL_SUCCESS);
        	}
        }
        session.commit(); 
	}
	
	private JSONObject submitAndHandleJob(String livyUrl, String sessionId, String payload){
		String statementUrl = livyUrl+"/sessions/"+sessionId+"/statements";
		JSONObject output = null;
		Map<String,String> headers = new HashMap<String,String>();
		headers.put("Content-Type", "application/json");
		headers.put("X-Requested-By", "user");
		
		getLogger().debug("********** submitAndHandleJob() Submitting Job to Spark via: " + statementUrl);
		getLogger().debug("********** submitAndHandleJob() Job payload: " + payload);
		try {
			JSONObject jobInfo = readJSONObjectFromUrlPOST(statementUrl, headers, payload);
			getLogger().debug("********** submitAndHandleJob() Job Info: " + jobInfo);
			String statementId = String.valueOf(jobInfo.getInt("id"));
			statementUrl = statementUrl+"/"+statementId;
			jobInfo = readJSONObjectFromUrl(statementUrl, headers);
			String jobState = jobInfo.getString("state"); 
			
			getLogger().debug("********** submitAndHandleJob() New Job Info: "+jobInfo);
			Thread.sleep(1000);
			if(jobState.equalsIgnoreCase("available")){
				getLogger().debug("********** submitAndHandleJob() Job status is: "+jobState+". returning output...");
				output = jobInfo.getJSONObject("output").getJSONObject("data");
			}else if(jobState.equalsIgnoreCase("running") || jobState.equalsIgnoreCase("waiting")){
				while(!jobState.equalsIgnoreCase("available")){
					getLogger().debug("********** submitAndHandleJob() Job status is: "+jobState+". Wating for job to complete...");
					Thread.sleep(1000);
					jobInfo = readJSONObjectFromUrl(statementUrl, headers);
					jobState = jobInfo.getString("state"); 
				}
				output = jobInfo.getJSONObject("output").getJSONObject("data");
			}else if(jobState.equalsIgnoreCase("error") || jobState.equalsIgnoreCase("cancelled") || jobState.equalsIgnoreCase("cancelling")){
				getLogger().debug("********** Job status is: "+jobState+". Job did not complete due to error or has been cancelled. Check SparkUI for details.");
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return output;
	}
	
	private JSONObject readJSONObjectFromUrlPOST(String urlString, Map<String,String> headers, String payload) throws IOException, JSONException {
		JSONObject jsonObject = null;
		try {
            URL url = new URL (urlString);
            
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);
            
            for (Map.Entry<String, String> entry : headers.entrySet()){
            	connection.setRequestProperty(entry.getKey(), entry.getValue());
            }
            
            OutputStream os = connection.getOutputStream();
    		os.write(payload.getBytes());
    		os.flush();
            
            if (connection.getResponseCode() != 200 && connection.getResponseCode() != 201){
    			throw new RuntimeException("Failed : HTTP error code : " + connection.getResponseCode() + " : " + connection.getResponseMessage());
    		}
            
            InputStream content = (InputStream)connection.getInputStream();
            BufferedReader rd = new BufferedReader(new InputStreamReader(content, Charset.forName("UTF-8")));
  	      	String jsonText = readAll(rd);
  	      	jsonObject = new JSONObject(jsonText);
        } catch(Exception e) {
            e.printStackTrace();
        }
        return jsonObject;
    }
	
	private JSONObject readJSONObjectFromUrl(String urlString, Map<String,String> headers) throws IOException, JSONException {
		JSONObject json = null;
		try {
            URL url = new URL (urlString);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            for (Map.Entry<String, String> entry : headers.entrySet()){
            	connection.setRequestProperty(entry.getKey(), entry.getValue());
            }
            connection.setRequestMethod("GET");
            connection.setDoOutput(true);
            //connection.setRequestProperty  ("Authorization", "Basic " + encoding);
            InputStream content = (InputStream)connection.getInputStream();
            BufferedReader rd = new BufferedReader(new InputStreamReader(content, Charset.forName("UTF-8")));
  	      	String jsonText = readAll(rd);
  	      	json = new JSONObject(jsonText);
        } catch(Exception e) {
            e.printStackTrace();
        }
        return json;
    }
	
	private String readAll(Reader rd) throws IOException {
	    StringBuilder sb = new StringBuilder();
	    int cp;
	    while ((cp = rd.read()) != -1) {
	      sb.append((char) cp);
	    }
	    return sb.toString();
	}
}