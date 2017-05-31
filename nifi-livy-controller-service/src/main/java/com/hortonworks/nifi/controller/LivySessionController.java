package com.hortonworks.nifi.controller;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.hortonworks.nifi.controller.api.LivySessionService;

@Tags({"Livy","REST","Spark"})
@CapabilityDescription("Manages pool of Spark sessions over HTTP")
public class LivySessionController extends AbstractControllerService implements LivySessionService{	
	private String livyUrl;
	private int sessionPoolSize;
	private String sessionKind;
	private Map<Integer, Object> sessions = new HashMap<Integer,Object>();
    
	public static final PropertyDescriptor LIVY_HOST = new PropertyDescriptor.Builder()
            .name("livy_host")
            .description("Livy Host")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();
	
	public static final PropertyDescriptor LIVY_PORT = new PropertyDescriptor.Builder()
            .name("livy_port")
            .description("Livy Port")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
	
	public static final PropertyDescriptor SESSION_POOL_SIZE = new PropertyDescriptor.Builder()
            .name("session_pool_size")
            .description("Number of sessions to keep open")
            .required(true)
            .defaultValue("2")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
	
	public static final PropertyDescriptor SESSION_KIND = new PropertyDescriptor.Builder()
            .name("session_kind")
            .description("The kind of Spark session to start")
            .required(true)
            .allowableValues("spark","pyspark","pyspark3","sparkr")
            .defaultValue("spark")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
	
	public static final PropertyDescriptor JARS = new PropertyDescriptor.Builder()
            .name("jars")
            .description("JARs to be used in the Spark session.")
            .required(false)
            //.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
	
	public static final PropertyDescriptor FILES = new PropertyDescriptor.Builder()
            .name("files")
            .description("Files to be used in the Spark session.")
            .required(false)
            //.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
	
	 private static final List<PropertyDescriptor> properties;
	
	static{
		final List<PropertyDescriptor> props = new ArrayList<>();
	    props.add(LIVY_HOST);
	    props.add(LIVY_PORT);
	    props.add(SESSION_POOL_SIZE);
	    props.add(SESSION_KIND);
	    props.add(JARS);
	    props.add(FILES);
	    
	    properties = Collections.unmodifiableList(props);
	}
	
	@Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

	@OnEnabled
	public void onConfigured(final ConfigurationContext context) throws InitializationException{
		getLogger().info("********** Starting Livy Session Controller Service...");
	   
		final String livyHost = context.getProperty(LIVY_HOST).getValue();
		final String livyPort = context.getProperty(LIVY_PORT).getValue();
		final String session_pool_size = context.getProperty(SESSION_POOL_SIZE).getValue();
		final String session_kind = context.getProperty(SESSION_KIND).getValue();
		final String jars = context.getProperty(JARS).getValue();
		final String files  = context.getProperty(FILES).getValue();
		
		livyUrl = "http://"+livyHost+":"+livyPort;
		sessionKind = session_kind;
		sessionPoolSize = Integer.valueOf(session_pool_size);
		
		new Thread(new Runnable() {
	        public void run(){
	            while(true){
	            	manageSessions();
	            	try {
						Thread.sleep(2000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
	            }
	        }
	    }).start();
	}
	
	public Map<String,String> getSession(){
		Map<String,String> sessionMap = new HashMap<String,String>();
		getLogger().debug("********** getSession() Aquiring session...");
		getLogger().debug("********** getSession() Session Cache: " + sessions);
		try {
			for(int sessionId: sessions.keySet()){
				JSONObject currentSession = (JSONObject)sessions.get(sessionId);
				String state = currentSession.getString("state");
				if(state.equalsIgnoreCase("idle")){
					sessionMap.put("sessionId",String.valueOf(sessionId));
					sessionMap.put("livyUrl",livyUrl);
				}
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
		getLogger().debug("********** getSession() Returning idle sessions: " + sessionMap);
		return sessionMap;
	}
	
	private void manageSessions(){
		int idleSessions=0;
		JSONObject newSessionInfo = null;
		Map<Integer,JSONObject> sessionsInfo = null;
		Map<String,String> headers = new HashMap<String,String>();
		headers.put("Content-Type", "application/json");
		headers.put("X-Requested-By", "user");
		
		try {
			getLogger().debug("********** manageSessions() aquiring list of sessions...");
			sessionsInfo = listSessions();			
			for(int sessionId: sessions.keySet()){
				JSONObject currentSession = (JSONObject)sessions.get(sessionId);
				if(sessionsInfo.containsKey(sessionId)){	
					getLogger().debug("********** manageSessions() updating current session: " + currentSession);
					String state = currentSession.getString("state");
					if(state.equalsIgnoreCase("idle")){
						//Keep track of how many sessions are in an idle state and thus available
						idleSessions++;
						sessions.put(sessionId,sessionsInfo.get(sessionId));
						//Remove session from session list source of truth snapshot since it has been dealt with
						sessionsInfo.remove(sessionId);
					}else if(state.equalsIgnoreCase("busy")||state.equalsIgnoreCase("starting")){
						//Update status of existing sessions
						sessions.put(sessionId,sessionsInfo.get(sessionId));
						//Remove session from session list source of truth snapshot since it has been dealt with
						sessionsInfo.remove(sessionId);
					}else{
						//Prune sessions whose state is: not_started, shutting_down, error, dead, success (successfully stopped)
						sessions.remove(sessionId);
						//Remove session from session list source of truth snapshot since it has been dealt with
						sessionsInfo.remove(sessionId);
					}
				}else{
					//Prune sessions that no longer exist
					sessions.remove(sessionId);
					//Remove session from session list source of truth snapshot since it has been dealt with
					sessionsInfo.remove(sessionId);
				}
			}
			//Update Session Cache with any sessions that remain in the source of truth snapshot since they were not created by this thread
			for(int sessionId: sessionsInfo.keySet()){
				sessions.put(sessionId,sessionsInfo.get(sessionId));
			}
			
			int numSessions = sessionsInfo.size();
			//Open new sessions equal to the number requested by session_pool_size
			if(numSessions==0){
				getLogger().debug("********** manageSessions() There are no available sessions, creating...");
				for(int i=0; i<sessionPoolSize; i++){
					newSessionInfo = openSession();
					sessions.put(newSessionInfo.getInt("id"), newSessionInfo);
					getLogger().debug("********** manageSessions() Registered new session: " + newSessionInfo);
				}
			}else{
				//Open one new session if there are no idle sessions
				if(idleSessions==0){
					getLogger().debug("********** manageSessions() There are no idle sessions, creating...");
					newSessionInfo = openSession();
					sessions.put(newSessionInfo.getInt("id"), newSessionInfo);
					getLogger().debug("********** manageSessions() Registered new session: " + newSessionInfo);
				}
				//Open more sessions if number of sessions is less than target pool size
				if(numSessions < sessionPoolSize){
					getLogger().debug("********** manageSessions() Need more sessions to equal requested pool size, creating...");
					for(int i=0; i<sessionPoolSize-numSessions; i++){
						newSessionInfo = openSession();
						sessions.put(newSessionInfo.getInt("id"), newSessionInfo);
						getLogger().debug("********** manageSessions() Registered new session: " + newSessionInfo);
					}
				}
			}
			
			getLogger().debug("********** manageSessions() Updated map of sessions: " + sessions);
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}
	
	private Map<Integer,JSONObject> listSessions(){
		String sessionsUrl = livyUrl+"/sessions";
		int numSessions = 0;
		JSONObject sessionsInfo = null;
		Map<Integer,JSONObject> sessionsMap = new HashMap<Integer,JSONObject>();
		Map<String,String> headers = new HashMap<String,String>();
		headers.put("Content-Type", "application/json");
		headers.put("X-Requested-By", "user");
		try {
			sessionsInfo = readJSONFromUrl(sessionsUrl, headers);
			numSessions = sessionsInfo.getInt("total");
			for(int i=0;i<=numSessions; i++){
				int currentSessionId = sessionsInfo.getJSONArray("sessions").getJSONObject(i).getInt("id");
				JSONObject currentSession = sessionsInfo.getJSONArray("sessions").getJSONObject(i);
				sessionsMap.put(currentSessionId,currentSession);
			}	
		} catch (IOException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		}
		
		return sessionsMap;
	}
	
	private JSONObject getSessionInfo(int sessionId){
		String sessionUrl = livyUrl+"/sessions/"+sessionId;
		JSONObject sessionInfo = null;
		Map<String,String> headers = new HashMap<String,String>();
		headers.put("Content-Type", "application/json");
		headers.put("X-Requested-By", "user");
		try {
			sessionInfo = readJSONFromUrl(sessionUrl, headers);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		}
		
		return sessionInfo;
	}
	
	private JSONObject openSession(){
		String sessionsUrl = livyUrl+"/sessions";
		String payload = "{\"kind\":\""+sessionKind+"\"}";
		JSONObject newSessionInfo = null;
		Map<String,String> headers = new HashMap<String,String>();
		headers.put("Content-Type", "application/json");
		headers.put("X-Requested-By", "user");
		
		try {
			newSessionInfo = readJSONObjectFromUrlPOST(sessionsUrl, headers, payload);
			getLogger().debug("********** openSession() Created new sessions: " + newSessionInfo);
			Thread.sleep(500);
			while(!newSessionInfo.getString("state").equalsIgnoreCase("idle")){
				getLogger().debug("********** openSession() Wating for session to start...");
				newSessionInfo = getSessionInfo(newSessionInfo.getInt("id"));
				getLogger().debug("********** openSession() newSessionInfo: " + newSessionInfo);
				Thread.sleep(1000);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		return newSessionInfo;
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
	
	private JSONArray readJSONArrayFromUrlPOST(String urlString, Map<String,String> headers, String payload) throws IOException, JSONException {
		JSONArray jsonArray = null;
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
            
            if (connection.getResponseCode() != 200) {
    			throw new RuntimeException("Failed : HTTP error code : " + connection.getResponseCode() + " : " + connection.getResponseMessage());
    		}
            
            InputStream content = (InputStream)connection.getInputStream();
            BufferedReader rd = new BufferedReader(new InputStreamReader(content, Charset.forName("UTF-8")));
  	      	String jsonText = readAll(rd);
  	      	jsonArray = new JSONArray(jsonText);
        } catch(Exception e) {
            e.printStackTrace();
        }
        return jsonArray;
    }
	
	private JSONObject readJSONFromUrl(String urlString, Map<String,String> headers) throws IOException, JSONException {
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
	
	private JSONArray readJSONArrayFromUrl(String urlString, Map<String,String> headers) throws IOException, JSONException {
		JSONArray jsonArray = null;
		try {
            URL url = new URL (urlString);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            for (Map.Entry<String, String> entry : headers.entrySet()){
            	connection.setRequestProperty(entry.getKey(), entry.getValue());
            }
            connection.setRequestMethod("GET");
            connection.setDoOutput(true);
            InputStream content = (InputStream)connection.getInputStream();
            BufferedReader rd = new BufferedReader(new InputStreamReader(content, Charset.forName("UTF-8")));
  	      	String jsonText = readAll(rd);
  	      	jsonArray = new JSONArray(jsonText);
        } catch(Exception e) {
            e.printStackTrace();
        }
        return jsonArray;
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
