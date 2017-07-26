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
import java.util.concurrent.ConcurrentHashMap;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.hortonworks.nifi.controller.api.LivySessionService;

@Tags({"Livy","REST","Spark"})
@CapabilityDescription("Manages pool of Spark sessions over HTTP")
public class LivySessionController extends AbstractControllerService implements LivySessionService{	
	private String livyUrl;
	private int sessionPoolSize;
	private String controllerKind;
	private String jars;
	private Map<Integer, JSONObject> sessions = new ConcurrentHashMap<Integer,JSONObject>();
	private Thread livySessionManagerThread = null;
	private boolean enabled = true;
    
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
            .addValidator((new StandardValidators.FileExistsValidator(true)))
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
		
		this.livyUrl = "http://"+livyHost+":"+livyPort;
		this.controllerKind = session_kind;
		this.jars = jars;
		this.sessionPoolSize = Integer.valueOf(session_pool_size);
		this.enabled = true;
		
		livySessionManagerThread = new Thread(new Runnable() {
			public void run(){
	        	while(enabled){
	            	try {
	            		manageSessions();
						Thread.sleep(2000);
					} catch (InterruptedException e) {
						e.printStackTrace();
						getLogger().debug("********** "+Thread.currentThread().getName()
										+ " run() Iterrupt Status: "+ Thread.currentThread().isInterrupted());
						enabled = false;
					}
	            }
	        }
	    });
		livySessionManagerThread.setName("Livy-Session-Manager-"+controllerKind);
		livySessionManagerThread.start();
	}
	
	@OnDisabled
    public void shutdown() {
		try {
			enabled = false;
			livySessionManagerThread.interrupt();
			livySessionManagerThread.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
    }
	
	public Map<String,String> getSession(){
		Map<Integer,JSONObject> sessionsCopy = new HashMap<Integer,JSONObject>();
		Map<String,String> sessionMap = new HashMap<String,String>();
		getLogger().debug("********** getSession() Aquiring session...");
		getLogger().debug("********** getSession() Session Cache: " + sessions);
		sessionsCopy = sessions;
		try {
			for(int sessionId: sessionsCopy.keySet()){
				JSONObject currentSession = (JSONObject)sessions.get(sessionId);
				String state = currentSession.getString("state");
				String sessionKind = currentSession.getString("kind");
				if(state.equalsIgnoreCase("idle") && sessionKind.equalsIgnoreCase(controllerKind)){
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
	
	private void manageSessions() throws InterruptedException{
		int idleSessions=0;
		JSONObject newSessionInfo = null;
		Map<Integer,JSONObject> sessionsInfo = null;
		Map<Integer,JSONObject> sessionsCopy = new HashMap<Integer,JSONObject>();
		Map<String,String> headers = new HashMap<String,String>();
		headers.put("Content-Type", "application/json");
		headers.put("X-Requested-By", "user");
		
		try {
			getLogger().debug("********** manageSessions() aquiring list of sessions...");
			sessionsInfo = listSessions();
			if(sessions.isEmpty()){
				getLogger().debug("********** manageSessions() the active session list is empty, populating from aquired list...");
				sessions.putAll(sessionsInfo);
			}
			//sessionsCopy = sessions;
			Iterator<Integer> sessionIterator = new ArrayList<>(sessions.keySet()).iterator();
			//for(int sessionId: sessions.keySet()){
			while(sessionIterator.hasNext()){
				int sessionId = sessionIterator.next();
				JSONObject currentSession = (JSONObject)sessions.get(sessionId);
				getLogger().debug("********** manageSessions() Updating current session: " + currentSession);
				if(sessionsInfo.containsKey(sessionId)){	
					String state = currentSession.getString("state");
					String sessionKind = currentSession.getString("kind");
					getLogger().debug("********** manageSessions() controler kind: " + controllerKind);
					getLogger().debug("********** manageSessions() session kind: " + sessionKind);
					getLogger().debug("********** manageSessions() session state: " + state);
					if(state.equalsIgnoreCase("idle") && sessionKind.equalsIgnoreCase(controllerKind)){
						//Keep track of how many sessions are in an idle state and thus available
						getLogger().debug("********** manageSessions() found " + state + " session of kind " + sessionKind);
						idleSessions++;
						getLogger().debug("********** manageSessions() storing session in session pool...");
						sessions.put(sessionId,sessionsInfo.get(sessionId));
						//Remove session from session list source of truth snapshot since it has been dealt with
						sessionsInfo.remove(sessionId);
						getLogger().debug("********** manageSessions() currently session pool looks like this: " + sessions);
					}else if((state.equalsIgnoreCase("busy")||state.equalsIgnoreCase("starting")) && sessionKind.equalsIgnoreCase(controllerKind)){
						//Update status of existing sessions
						getLogger().debug("********** manageSessions() found " + state + " session of kind " + sessionKind);
						getLogger().debug("********** manageSessions() storing session in session pool...");
						sessions.put(sessionId,sessionsInfo.get(sessionId));
						//Remove session from session list source of truth snapshot since it has been dealt with
						sessionsInfo.remove(sessionId);
						getLogger().debug("********** manageSessions() currently session pool looks like this: " + sessions);
					}else{
						//Prune sessions of kind != controllerKind and whose state is: 
						//not_started, shutting_down, error, dead, success (successfully stopped)
						getLogger().debug("********** manageSessions() found " + state + " session of kind " + sessionKind);
						getLogger().debug("********** manageSessions() session is either of wrong kind or in an bad state...");
						sessions.remove(sessionId);
						//Remove session from session list source of truth snapshot since it has been dealt with
						sessionsInfo.remove(sessionId);
						getLogger().debug("********** manageSessions() currently session pool looks like this: " + sessions);
					}
				}else{
					//Prune sessions that no longer exist
					getLogger().debug("********** manageSessions() session exists in session pool but not in source snapshot, removing from pool...");
					sessions.remove(sessionId);
					//Remove session from session list source of truth snapshot since it has been dealt with
					sessionsInfo.remove(sessionId);
					getLogger().debug("********** manageSessions() currently session pool looks like this: " + sessions);
				}
			}
			//Update Session Cache with any sessions that remain in the source of truth snapshot since they were not created by this thread
			//for(int sessionId: sessionsInfo.keySet()){
			//	sessions.put(sessionId,sessionsInfo.get(sessionId));
			//}
			getLogger().debug("********** manageSessions() currently session pool looks like this: " + sessions);
			int numSessions = sessions.size();
			getLogger().debug("********** manageSessions() There are " + numSessions+ " sessions in the pool");
			//Open new sessions equal to the number requested by session_pool_size
			if(numSessions==0){
				getLogger().debug("********** manageSessions() There are " + numSessions+ " sessions in the pool, creating...");
				for(int i=0; i<sessionPoolSize; i++){
					newSessionInfo = openSession();
					sessions.put(newSessionInfo.getInt("id"), newSessionInfo);
					getLogger().debug("********** manageSessions() Registered new session: " + newSessionInfo);
				}
			}else{
				//Open one new session if there are no idle sessions
				if(idleSessions==0){
					getLogger().debug("********** manageSessions() There are " + numSessions+ " sessions in the pool but none of them are idle sessions, creating...");
					newSessionInfo = openSession();
					sessions.put(newSessionInfo.getInt("id"), newSessionInfo);
					getLogger().debug("********** manageSessions() Registered new session: " + newSessionInfo);
				}
				//Open more sessions if number of sessions is less than target pool size
				if(numSessions < sessionPoolSize){
					getLogger().debug("********** manageSessions() There are " + numSessions+ ", need more sessions to equal requested pool size of "+sessionPoolSize+", creating...");
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
			numSessions = sessionsInfo.getJSONArray("sessions").length();
			getLogger().debug("********** listSessions() Number of sessions: " + numSessions);
			for(int i=0;i<numSessions; i++){
				getLogger().debug("********** listSessions() Updated map of sessions: " + sessionsMap);
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
	
	private JSONObject openSession() throws InterruptedException{
		JSONObject newSessionInfo = null;
		try {
			String sessionsUrl = livyUrl+"/sessions";
			String payload = null;
			if(jars != null){
				String[] jarsArray = jars.split(",");
				ObjectMapper mapper = new ObjectMapper();
				String jarsJsonArray = mapper.writeValueAsString(jarsArray);
				payload = "{\"kind\":\""+controllerKind+"\",\"jars\":"+jarsJsonArray+"}";
			}else{
				payload = "{\"kind\":\""+controllerKind+"\"}";
			}
			getLogger().debug("********** openSession() Session Payload: " + payload);
			Map<String,String> headers = new HashMap<String,String>();
			headers.put("Content-Type", "application/json");
			headers.put("X-Requested-By", "user");
		
			newSessionInfo = readJSONObjectFromUrlPOST(sessionsUrl, headers, payload);
			getLogger().debug("********** openSession() Created new sessions: " + newSessionInfo);
			Thread.sleep(1000);
			while(newSessionInfo.getString("state").equalsIgnoreCase("starting")){
				getLogger().debug("********** openSession() Wating for session to start...");
				newSessionInfo = getSessionInfo(newSessionInfo.getInt("id"));
				getLogger().debug("********** openSession() newSessionInfo: " + newSessionInfo);
				Thread.sleep(1000);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		} catch (Exception e) {
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
