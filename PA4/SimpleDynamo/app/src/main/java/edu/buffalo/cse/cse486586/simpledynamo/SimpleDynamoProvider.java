package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Environment;
import android.telephony.TelephonyManager;
import android.util.Log;

class Constants{
	public static Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
	public static int socketTimeoutInMilliSeconds = 3000;
	public static final String KEY_FIELD = "key";
	public static final String VALUE_FIELD = "value";
	public static final String VERSION = "VERSION";
	public static final String AVD_5554 = "5554";
	public static final String AVD_5556 = "5556";
	public static final String AVD_5558 = "5558";
	public static final String AVD_5560 = "5560";
	public static final String AVD_5562 = "5562";
	public static final Map<String, String[]> REPLICATORS_MAP = initializeReplicators();
	public static final Map<String, String> PREDECESSORS_MAP = createPredecessorMap();
	public static final Map<String, String> SUCCESSORS_MAP = createSuccessorMap();
	public static final String[] ALL_NODE_IDS = {AVD_5554, AVD_5556, AVD_5558, AVD_5560, AVD_5562 };
	public static final String INSERT_REQ_TO_COORDINATOR = "INSERT_REQ_TO_COORDINATOR";
	public static final String INSERT_ON_COORDINATOR_FAILURE = "INSERT_ON_COORDINATOR_FAILURE";
	public static final String REPLICATE_DATA = "REPLICATE_DATA";
	public static final String SOCKET_CLOSE_ACK = "SOCKET_CLOSE_ACK";
	public static final String GET_REQUEST = "GET_REQUEST";
	public static final String GET_ON_COORDINATOR_FAILURE = "GET_ON_COORDINATOR_FAILURE";
	public static final String GET_FROM_REPLICA = "GET_FROM_REPLICA";
	public static final String GET_REQUEST_RESP = "GET_REQUEST_RESP";
	public static final String DELETE_REQUEST = "DELETE_REQUEST";
	public static final String DELETE_REPLICATED_DATA = "DELETE_REPLICATED_DATA";
    public static final String PERFORM_RECOVERY = "PERFORM_RECOVERY";
	static final int SERVER_PORT = 10000;

	private static Uri buildUri(String scheme, String authority) {
		if(mUri != null){
			return mUri;
		}
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		mUri = uriBuilder.build();
		return mUri;
	}

	private static Map<String, String[]> initializeReplicators(){
		Map<String, String[]> map = new HashMap<String, String[]>();
		map.put(AVD_5554, new String[]{AVD_5558, AVD_5560});
		map.put(AVD_5556, new String[]{AVD_5554, AVD_5558});
		map.put(AVD_5558, new String[]{AVD_5560, AVD_5562});
		map.put(AVD_5560, new String[]{AVD_5562, AVD_5556});
		map.put(AVD_5562, new String[]{AVD_5556, AVD_5554});
		return map;
	}

	private static Map<String, String> createPredecessorMap(){
		Map<String, String> map = new HashMap<String, String>();
		map.put(AVD_5554, AVD_5556);
		map.put(AVD_5556, AVD_5562);
		map.put(AVD_5558, AVD_5554);
		map.put(AVD_5560, AVD_5558);
		map.put(AVD_5562, AVD_5560);
		return map;
	}

	private static Map<String, String> createSuccessorMap(){
		Map<String, String> map = new HashMap<String, String>();
		map.put(AVD_5554, AVD_5558);
		map.put(AVD_5556, AVD_5554);
		map.put(AVD_5558, AVD_5560);
		map.put(AVD_5560, AVD_5562);
		map.put(AVD_5562, AVD_5556);
		return map;
	}

	public static String LOCAL_QUERY = "@";
	public static String GLOBAL_QUERY = "*";
}

public class SimpleDynamoProvider extends ContentProvider {
	private String myPort;
	private String hashedNodeId;
	private String myNodeId;
	private String predecessor1NodeId;
	private String predecessor2NodeId;
	private DatabaseHelper db;
	public static Map<String, Integer> versionMap = new HashMap<String, Integer>();
	public static final Object globalSyncObj = new Object();
	public static final Object serverSyncObj = new Object();
	public static AtomicBoolean canStartServer = new AtomicBoolean(false);

	@Override
	public boolean onCreate() {
		try {
			this.hashedNodeId = getHashedNodeId(getMyPort());
			this.myNodeId = getNodeId(getMyPort());
			this.predecessor1NodeId = Constants.PREDECESSORS_MAP.get(this.myNodeId);
			this.predecessor2NodeId = Constants.PREDECESSORS_MAP.get(this.predecessor1NodeId);


			if(db == null){
				db =  new DatabaseHelper(getContext());
//				synchronized (dbSyncObject){
//					db.deleteAllInDB();
//					Log.i("On Recovery", "Deleted everything from database..");
//				}
			}
			sendMessageThroughClientTask(Constants.PERFORM_RECOVERY + "-" + myPort);
			try {
				while (!canStartServer.get()){
					Log.i("On Create", "Recovery has not started.. waiting to start server task");
				}
				ServerSocket serverSocket = new ServerSocket(Constants.SERVER_PORT);
				new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
			} catch (IOException e) {
				Log.e("On create", "Can't create a ServerSocket");
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	public void getLostDataOnRecovery(){
		try{
			synchronized (globalSyncObj){
				synchronized (serverSyncObj){
					canStartServer.set(true);
					String predecessorNodeId = Constants.PREDECESSORS_MAP.get(this.myNodeId);
					String successorNodeId = Constants.SUCCESSORS_MAP.get(this.myNodeId);
					Log.i("On Recovery", "Contacting predecessor " + predecessorNodeId + " successor " + successorNodeId + " for data.");
					String getMsg = Constants.GET_REQUEST + "-" + Constants.LOCAL_QUERY;
					String[] neighbours = new String[]{predecessorNodeId, successorNodeId};
					for(String nodeId: neighbours){
						try{
							String[] response = connectAndGetResponse(getMsg,getPort(nodeId));
							if(response != null && response[0].equals(Constants.GET_REQUEST_RESP)){
								if(response.length > 1){
									Log.i("On Recovery", "Got response after recovery.." + response[1]);
									checkAndInsertInDBOnRecovery(getMatrixCursorFromString(response[1], true));
								}
							}
						}
						catch (Exception e){
							Log.i("On Recovery", "Error during recovery while connecting to " + nodeId);
							e.printStackTrace();
							String nextOrPrevNode;
							if(nodeId.equals(predecessorNodeId)){
								Log.i("On Recovery", "Contacting Predecessors Predecessor " + nodeId);
								nextOrPrevNode = Constants.PREDECESSORS_MAP.get(predecessorNodeId);
							}
							else {
								Log.i("On Recovery", "Contacting Successors Successor " + nodeId);
								nextOrPrevNode = Constants.SUCCESSORS_MAP.get(successorNodeId);
							}
							try {
								String[] response = connectAndGetResponse(getMsg,getPort(nextOrPrevNode));
								if(response != null && response[0].equals(Constants.GET_REQUEST_RESP)){
									if(response.length > 1){
										Log.i("On Recovery", "Got response after recovery.." + response[1]);
										checkAndInsertInDBOnRecovery(getMatrixCursorFromString(response[1], true));
									}
								}
							}
							catch (Exception ex){
								Log.i("On Recovery", "Error occurred while contacting nextOrPrevNode.." + nextOrPrevNode);
							}
						}
					}
				}
			}
		}
		catch (Exception e){
			Log.e("On Recovery", "Error during recovery.");
			e.printStackTrace();
		}
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		synchronized (globalSyncObj){
			if(selection.equalsIgnoreCase(Constants.GLOBAL_QUERY)){
				Log.i("Query","(*) selection query executed. " +selection);
				for(String nodeId: Constants.ALL_NODE_IDS){
					try{
						if(nodeId.equals(myNodeId)){
							Log.i("For loop.", "Deleting my own db..");
							db.deleteAllInDB();
							versionMap = new HashMap<String, Integer>();
						}
						else{
							String queryMsg = Constants.DELETE_REQUEST + "-" + Constants.LOCAL_QUERY;
							connectAndGetResponse(queryMsg, getPort(nodeId));
							Log.i("For loop.", "Received response from server for delete: " + nodeId);
						}
					}
					catch (Exception e){
						e.printStackTrace();
						Log.e("Err in client PropResp", "PropResp: all exceptions");
					}
				}
			}
			if(selection.equalsIgnoreCase(Constants.LOCAL_QUERY)){
				Log.i("Delete","@ delete executed.");
				db.deleteAllInDB();
				versionMap = new HashMap<String, Integer>();
				return 0;
			}
			else{
				if(isInKeySpace(getNodeId(getMyPort()), genHash(selection))){
					Log.i("Delete","In my key space.. so deleting.." + selection);
					db.delete(uri.toString(),selection,selectionArgs);
					versionMap.remove(selection);
					deleteDataInReplicas(myNodeId,selection);
				}
				else{
					String coordinatorNodeId = getCoordinator(genHash(selection));
					String coordinatorPort = getPort(coordinatorNodeId);
					String queryMsg = Constants.DELETE_REQUEST  + "-" + selection;
					try {
						Log.i("Delete","Not in my key space.. so routing the delete req to coordinator... " + selection);
						connectAndGetResponse(queryMsg, coordinatorPort);
						deleteDataInReplicas(coordinatorNodeId,selection);
					}
					catch (SocketTimeoutException e) {
						Log.e("Err in delete","delete: SocketTimeoutException on port: " + coordinatorPort);
						e.printStackTrace();
						handleFailure(queryMsg, coordinatorPort);
					} catch (SocketException e) {
						Log.e("Err in delete","delete: SocketException on port: " + coordinatorPort);
						e.printStackTrace();
						handleFailure(queryMsg, coordinatorPort);
					} catch (IOException e){
						Log.e("Err in delete", "delete: IO exceptions " +coordinatorPort);
						e.printStackTrace();
						handleFailure(queryMsg, coordinatorPort);
					}catch (Exception e){
						Log.e("Err in delete", "delete: all exceptions " +coordinatorPort);
					}
				}
			}
			return 0;
		}
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	public Uri insert(Uri uri, ContentValues values) {
		Log.i("Insert", "Trying to acquire lock");
		synchronized (globalSyncObj){
			String key = values.get(Constants.KEY_FIELD).toString();
			String value = values.get(Constants.VALUE_FIELD).toString();
			String hashedKey = genHash(key);
			Log.i("Insert", "Acquired lock..Performing insert for key.." + key + " value is " + value);
			String coordinator = getCoordinator(hashedKey);
			List<String> targetNodes = new ArrayList<String>();
			targetNodes.add(coordinator);
			targetNodes.addAll(Arrays.asList(Constants.REPLICATORS_MAP.get(coordinator)));
			Log.i("Insert", "Asking " + targetNodes + " to replicate data");
			for (String node:targetNodes) {
				if(node.equals(myNodeId)){
					Log.i("Insert", "Inserting in my own database..");
					db.insertOrUpdateData(key, value);
					addOrUpdateVersionMap(key);
				}
				else {
					String insertMsg = Constants.REPLICATE_DATA + "-" + key + "-" + value;
					try {
						Log.i("Insert", "Sent insert msg " + insertMsg + " to " + node);
						connectAndGetResponse(insertMsg, getPort(node));
					}
					catch (IOException e){
						Log.i("Insert", "Error while replicating data to node " + node + " for key " + key);
						e.printStackTrace();
					}
				}
			}
		}
		return null;
	}

	private void updateVersion(String key, int version) {
		versionMap.put(key,version);
	}

	private void addOrUpdateVersionMap(String key) {
		if(versionMap.containsKey(key)){
			Integer current = versionMap.get(key);
			current++;
			versionMap.put(key, current);
		}else{
			versionMap.put(key,1);
		}
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		synchronized (globalSyncObj){
			if(selection.equalsIgnoreCase(Constants.GLOBAL_QUERY)){
				Log.i("Query","(*, key, recurse) selection query executed. " +selection);
				MatrixCursor cursor = getConcatenatedCursor(db.getAllValues(), null, false);
				for(String nodeId: Constants.ALL_NODE_IDS){
					try{
						if(nodeId.equals(myNodeId)){
							continue;
						}
						Socket socket = getSocket(getPort(nodeId));
						DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
						PrintWriter pw = new PrintWriter(dataOutputStream, true);
						String queryMsg = Constants.GET_REQUEST + "-" + Constants.LOCAL_QUERY;
						pw.println(queryMsg);
						String[] msgContents = processServerResponse(socket, pw);
						Log.i("For loop.", "Received response from server in while loop: " + msgContents);
						if(msgContents[0].equals(Constants.GET_REQUEST_RESP)){
							String result = msgContents.length < 2 ? "" : msgContents[1];
							if(result.isEmpty()){
								Log.i("For loop.", "Got empty query response from server.. ");
								continue;
							}
							Log.i("For loop.", "Got query response from server.. ");
							MatrixCursor response;
							Log.i("For loop.", "The response string from the server is.. " + result);
							response = getMatrixCursorFromString(result, false);
							cursor = getConcatenatedCursor(response, cursor, false);
						}
					}
					catch (Exception e){
						Log.e("Err in query", "error connecting to " + nodeId);
					}
				}
				return cursor;
			}
			if(selection.equalsIgnoreCase(Constants.LOCAL_QUERY)){
				Log.i("Query","@ selection query executed.");
				Cursor result =  db.getAllValues();
				Log.i("Query","@ selection query executed and the count is " + result.getCount());
				return result;
			}
			else{
				Log.i("Query","selection query executed for value.. " + selection);
				MatrixCursor cursor = null;
				String coordinatorNodeId = getCoordinator(genHash(selection));
				if(isInKeySpace(this.myNodeId, genHash(selection))){
					Log.i("Query","Key is present in same node..");
					Cursor c =  db.getValue(uri.toString(),projection,selection,selectionArgs,null,null,sortOrder);
					cursor = getConcatenatedCursor(c, null, true);
				}
				else{
					String[] msgContents = null;
					String queryMsg = Constants.GET_REQUEST + "-" + selection;
					String coordinatorPort = getPort(coordinatorNodeId);
					try {
						Log.i("Query","Routing the query for " + selection + " to coordinator " + coordinatorNodeId);
						msgContents = connectAndGetResponse(queryMsg,coordinatorPort);
						Log.i("Query", "The response string from the coordinator is.. " + msgContents[1]);
						cursor =  getMatrixCursorFromString(msgContents[1], true);
					}
					catch (SocketTimeoutException e) {
						Log.e("Err in Query","In query SocketTimeoutException on port: " + coordinatorPort);
						e.printStackTrace();
					} catch (SocketException e) {
						Log.e("Err in Query","In query SocketException on port: " + coordinatorPort);
						e.printStackTrace();
					} catch (IOException e){
						Log.e("Err in Query", "In query IO exceptions " +coordinatorPort);
						e.printStackTrace();
					}catch (Exception e){
						Log.e("Err in Query", "In query all exceptions " +coordinatorPort);
						e.printStackTrace();
					}
				}
				MatrixCursor[] replicatorCursors  = new MatrixCursor[2];;
				String[] successors = Constants.REPLICATORS_MAP.get(coordinatorNodeId);
				String getMsg = Constants.GET_FROM_REPLICA + "-" + selection;
				for (int i=0; i< successors.length; i++){
					try{
						Log.i("Query", "Trying to connect to the successor "+ successors[i]);
						if(!successors[i].equals(this.myNodeId)){
							String[] contents = connectAndGetResponse(getMsg, getPort(successors[i]));
							Log.i("Query", "Got response from the successor "+ successors[i]);
							replicatorCursors[i] = getMatrixCursorFromString(contents[1], true);
						}
						else {
							Cursor c =  db.getValue(uri.toString(),projection,selection,selectionArgs,null,null,sortOrder);
							replicatorCursors[i] =  getConcatenatedCursor(c, null, true);
						}
					}
					catch (Exception e){
						Log.i("Query", "Error while contacting one of the successors with node Id: "+ successors[i]);
						e.printStackTrace();
					}
				}
				return getConcatenatedCursor(getLatestCursor(cursor, replicatorCursors),null, false);
			}
		}
	}

	private MatrixCursor getLatestCursor(MatrixCursor coordinatorCursor, MatrixCursor[] replicators){
		Integer[] versions = new Integer[]{0,0,0};
		Log.i("getLatestCursor", "Comparing the cursor versions");
		if(coordinatorCursor != null && coordinatorCursor.getCount() > 0 ){
			coordinatorCursor.moveToFirst();
			do{
				if(coordinatorCursor.getColumnIndex(Constants.VERSION) != -1){
					Integer version = Integer.valueOf(coordinatorCursor.getString(coordinatorCursor.getColumnIndex(Constants.VERSION)));
					Log.i("getLatestCursor", "Got the version from coordinatorCursor.." + version);
					versions[0] = version;
				}
				else{
					String key = coordinatorCursor.getString(coordinatorCursor.getColumnIndex(Constants.KEY_FIELD));
					if(versionMap.containsKey(key)){
						Log.i("getLatestCursor", "Got the version from version map.." + versionMap.get(key));
						versions[0] = versionMap.get(key);
					}
				}
			}while(coordinatorCursor.moveToNext());
		}
		Log.i("getLatestCursor", "The replicators length is " + replicators.length);
		if(replicators.length >0){
			for (int i=0; i< replicators.length; i++) {
				if(replicators[i] != null){
					replicators[i].moveToFirst();
					do {
						if (replicators[i].getColumnIndex(Constants.VERSION) != -1 && replicators[i].getString(replicators[i].getColumnIndex(Constants.VERSION)) != null) {
							Integer version = Integer.valueOf(replicators[i].getString(replicators[i].getColumnIndex(Constants.VERSION)));
							Log.i("getLatestCursor", "Got the version from replicator cursor.." + version);
							versions[i+1] = version;
						}
					} while (replicators[i].moveToNext());
				}
			}
		}
		List<Integer> lst = Arrays.asList(versions);
		Log.i("getLatestCursor", "The versions are " + lst);
		int maxIndex = lst.indexOf(Collections.max(lst));
		if(maxIndex == 0){
			Log.i("getLatestCursor", "Returning coordinator cursor " + versions[maxIndex]);
			return coordinatorCursor;
		}
		else{
			if(maxIndex == 1){
				return replicators[0];
			}
			else{
				return replicators[1];
			}
		}
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	private boolean canInsertOnRecovery(String hashedKey){
		return isInKeySpace(this.myNodeId, hashedKey)
				|| isInKeySpace(this.predecessor1NodeId, hashedKey)
				|| isInKeySpace(this.predecessor2NodeId, hashedKey);
	}

	// Always pass 555* as port and key should be hashed key..
	private boolean isInKeySpace(String nodeId, String key){
		String hashedPredecessorNodeId = genHash(Constants.PREDECESSORS_MAP.get(nodeId));
		String hashedNodeId = genHash(nodeId);
		Log.i("Compare", "NodeId "+ hashedNodeId + " predecessor... " + hashedPredecessorNodeId);
		if(key.compareTo(hashedNodeId) < 0 && key.compareTo(hashedPredecessorNodeId) > 0){
			Log.i("Compare", "Key "+ key + " is in my key space.." + hashedNodeId);
			return true;
		}
		if((hashedPredecessorNodeId.compareTo(hashedNodeId)) > 0 && ((key.compareTo(hashedNodeId)) < 0
				|| (key.compareTo(hashedPredecessorNodeId) > 0 ))){
			Log.i("Compare", "Key "+ key + " is in my key space.." + hashedNodeId);
			return  true;
		}
		Log.i("Compare", "Key not in my key space.. Route the request to coordinator.");
		return false;
	}

	// Always pass hashed Key to this method
	private String getCoordinator(String hashedKey){
		for(String nodeId: Constants.ALL_NODE_IDS) {
			if(!nodeId.equals(this.myNodeId) && isInKeySpace(nodeId, hashedKey)){
				Log.i("In getCoordinator", "The coordinator is " + nodeId);
				return nodeId;
			}
		}
		return this.myNodeId;
	}

	private String getHashedNodeId(String port){
		return genHash(Integer.toString(Integer.valueOf(port) / 2));
	}

	private String getNodeId(String port){
		return Integer.toString(Integer.valueOf(port) / 2);
	}

    private String getPort(String NodeId){
        return Integer.toString(Integer.valueOf(NodeId) *2);
    }

	private String getMyPort() {
		if(myPort != null && !myPort.isEmpty()){
			return myPort;
		}
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort =  String.valueOf((Integer.parseInt(portStr)) * 2);
		return myPort;
	}

	private void sendMessageThroughClientTask(String msg){
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
	}

	private Socket getSocket(String port) throws IOException {
		return new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
				Integer.parseInt(port));
	}

	private void checkAndInsertInDBOnRecovery(MatrixCursor cursor){
		int insertCounter = 0;
		List<String> ignoredHashedKeys = new ArrayList<String>();
		if(cursor != null && cursor.getCount() > 0){
			Log.i("insertRecovery", "Inside insert recovery method.. total cursor length is .." + cursor.getCount());
			cursor.moveToFirst();
			do{
				String key = cursor.getString(cursor.getColumnIndex(Constants.KEY_FIELD));
				String value = cursor.getString(cursor.getColumnIndex(Constants.VALUE_FIELD));
				int version = Integer.valueOf(cursor.getString(cursor.getColumnIndex(Constants.VERSION)));
				String hashedKey = genHash(key);
				if(canInsertOnRecovery(hashedKey) && (!versionMap.containsKey(key)|| versionMap.get(key) < version)){
					db.insertOrUpdateData(key, value);
					Log.i("insertRecovery", "Inserted key after recovery in db is.." + key + " The hash is " + hashedKey);
					updateVersion(key, version);
					insertCounter++;
				}
				else{
					ignoredHashedKeys.add(hashedKey);
				}
			}while (cursor.moveToNext());
		}
		Log.i("insertRecovery", "The total insert after recovery is: " + insertCounter);
		Log.i("insertRecovery", "The ignored keys hash values are: " + ignoredHashedKeys);
	}

	private MatrixCursor getConcatenatedCursor(Cursor cursor, MatrixCursor appendTo, boolean addVersion){
		MatrixCursor response;
		if(addVersion){
			response = new MatrixCursor(new String[]{Constants.KEY_FIELD, Constants.VALUE_FIELD, Constants.VERSION});
		}
		else{
			response = new MatrixCursor(new String[]{Constants.KEY_FIELD, Constants.VALUE_FIELD});
		}

		if(appendTo != null && appendTo.getCount() > 0){
			Log.i("getConcatenatedCursor","append to length is: " + appendTo.getCount());
			appendTo.moveToFirst();
			do{
				MatrixCursor.RowBuilder row = response.newRow();
				String key = appendTo.getString(appendTo.getColumnIndex(Constants.KEY_FIELD));
				row.add(Constants.KEY_FIELD, key);
				row.add(Constants.VALUE_FIELD, appendTo.getString(appendTo.getColumnIndex(Constants.VALUE_FIELD)));
				if(addVersion){
					if(appendTo.getColumnIndex(Constants.VERSION) == -1){
						if(versionMap.containsKey(key)){
							row.add(Constants.VERSION, versionMap.get(key).toString());
						}
						else{
							row.add(Constants.VERSION, Integer.toString(1));
						}

					}
					else{
						row.add(Constants.VERSION, appendTo.getString(appendTo.getColumnIndex(Constants.VERSION)));
					}
				}
			}while (appendTo.moveToNext());
		}
		if(cursor != null && cursor.getCount() > 0 ){
			Log.i("getConcatenatedCursor","Cursor length is: " + cursor.getCount());
			cursor.moveToFirst();
			do{
				MatrixCursor.RowBuilder row = response.newRow();
				row.add(Constants.KEY_FIELD, cursor.getString(cursor.getColumnIndex(Constants.KEY_FIELD)));
				row.add(Constants.VALUE_FIELD, cursor.getString(cursor.getColumnIndex(Constants.VALUE_FIELD)));
				if(addVersion){
					String key = cursor.getString(cursor.getColumnIndex(Constants.KEY_FIELD));
					if(cursor.getColumnIndex(Constants.VERSION) == -1){
						Log.i("getConcatenatedCursor","The key present in map? " + versionMap.containsKey(key));
						if(versionMap.containsKey(key)){
							row.add(Constants.VERSION, versionMap.get(key).toString());
						}
						else{
							row.add(Constants.VERSION, Integer.toString(1));
						}
					}
					else{
						row.add(Constants.VERSION, cursor.getString(cursor.getColumnIndex(Constants.VERSION)));
					}
				}
			}while(cursor.moveToNext());
		}
		Log.i("getConcatenatedCursor","The concatenated Cursor length is: " + response.getCount());
		return response;
	}

	private String getStringFromCursor(Cursor cursor, boolean includeVersion){
		StringBuilder response = new StringBuilder();
		Log.i("getStringFromCursor","Cursor length is: " + cursor.getCount());
		if(cursor.getCount() > 0 ){
			cursor.moveToFirst();
			do{
				String key = cursor.getString(cursor.getColumnIndex(Constants.KEY_FIELD));
				response.append(key);
				response.append(":");
				response.append(cursor.getString(cursor.getColumnIndex(Constants.VALUE_FIELD)));
				if(includeVersion){
					response.append(":");
					 if(versionMap.containsKey(key)) {
						response.append(versionMap.get(key));
					 }
					 else{
						 response.append(1);
					 }
				}
				response.append("#");
			}while(cursor.moveToNext());
		}
		Log.i("getStringFromCursor","The response is: " + response.toString());
		return response.toString();
	}

	private MatrixCursor getMatrixCursorFromString(String data, boolean addVersion){
		MatrixCursor cursor;
		if(addVersion){
			cursor = new MatrixCursor(new String[]{Constants.KEY_FIELD, Constants.VALUE_FIELD, Constants.VERSION});
		}
		else{
			cursor = new MatrixCursor(new String[]{Constants.KEY_FIELD, Constants.VALUE_FIELD});
		}

		String[] dataItems = data.split("#");
		for (String dataItem : dataItems) {
			Log.i("getCursorFromString","The item is " + dataItem);
			String[] contentValues = dataItem.split(":");
			if(contentValues.length == 2){
				Log.i("getCursorFromString","The Key and Value is " + contentValues[0] + "**" + contentValues[1]);
				cursor.newRow()
						.add(Constants.KEY_FIELD, contentValues[0])
						.add(Constants.VALUE_FIELD, contentValues[1]);
			}
			if(contentValues.length == 3){
				Log.i("getCursorFromString","The size is 3 and The Key and Value is " + contentValues[0] + "**" + contentValues[1] + "**" + contentValues[2]);
				MatrixCursor.RowBuilder row = cursor.newRow();
				row.add(Constants.KEY_FIELD, contentValues[0]);
				row.add(Constants.VALUE_FIELD, contentValues[1]);
				if(addVersion){
					row.add(Constants.VERSION, contentValues[2]);
				}
			}
		}
		Log.i("getCursorFromString","The length of cursor returned  is " + cursor.getCount());
		return cursor;
	}

	private class ClientTask extends AsyncTask<String, Void, Void> {
		@Override
		protected synchronized Void doInBackground(String... msgs) {
			String[] messageContent = msgs[0].split("-");
			String msgType = messageContent[0];
			String port = messageContent[1];
			try {
				String insertMsg="";
				if(msgType.equals(Constants.INSERT_REQ_TO_COORDINATOR)){
					Log.i("replicateData", "Sending insert request to coordinator: " + port);
					insertMsg = Constants.INSERT_REQ_TO_COORDINATOR + "-" + messageContent[2] + "-" + messageContent[3];
				}
				if(msgType.equals(Constants.REPLICATE_DATA)){
					Log.i("replicateData", "Sending replication request to replicator: " + port);
					insertMsg = Constants.REPLICATE_DATA + "-" + messageContent[2] + "-" + messageContent[3];
				}
                if(msgType.equals(Constants.DELETE_REPLICATED_DATA)){
                    Log.i("replicateData", "Sending delete replication request to replicator: " + port);
                    insertMsg = Constants.DELETE_REPLICATED_DATA + "-" + messageContent[2];
                }
                if(msgType.equals(Constants.PERFORM_RECOVERY)){
                    Log.i("replicateData", "Performing recovery operations: ");
                    getLostDataOnRecovery();
                }
				if(!insertMsg.isEmpty()){
					connectAndGetResponse(insertMsg, port);
				}
			}
			catch (SocketTimeoutException e) {
				Log.e("Err in client PropResp","PropResp: SocketTimeoutException on port: " + port);
				e.printStackTrace();
				handleFailure(msgs[0], port);
			} catch (SocketException e) {
				Log.e("Err in client PropResp","PropResp: SocketException on port: " + port);
                e.printStackTrace();
				handleFailure(msgs[0], port);
			} catch (IOException e){
				Log.e("Err in client PropResp", "PropResp: IO exceptions " + port);
                e.printStackTrace();
				handleFailure(msgs[0], port);
			}catch (Exception e){
				Log.e("Err in client PropResp", "PropResp: all exceptions " + port);
			}
			return null;
		}

		@Override
		protected void onPostExecute (Void result){
			Log.i("In client","The task is completed..");
		}
	}

	private String[] connectAndGetResponse(String msg, String port) throws IOException {
		Socket socket = getSocket(port);
		socket.setTcpNoDelay(true);
		DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
		PrintWriter pw = new PrintWriter(dataOutputStream, true);
		pw.println(msg);
		socket.setSoTimeout(Constants.socketTimeoutInMilliSeconds);
		BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
		String response = br.readLine();
		Log.i("connection", "received response from server and closing socket.." + response);
		pw.flush();
		pw.close();
		socket.close();
		if(response == null){
			throw new SocketException("Throwing Custom socket exception ");
		}
		return response.split("-");
	}

	private String[] handleFailure(String message, String port){
		String[] response = null;
		try{
			String[] msgContents = message.split("-");
			String msgType = msgContents[0];
			if(msgType.equals(Constants.GET_REQUEST)){
				String getMsg = Constants.GET_ON_COORDINATOR_FAILURE + "-" + msgContents[1];
				String[] replicators = Constants.REPLICATORS_MAP.get(getNodeId(port));
				for (String nodeId : replicators) {
					try {
						Log.i("In handleFailure", "To handle coordinator failure, routing the get request to the coordinator's successor.." + nodeId);
						return connectAndGetResponse(getMsg, getPort(nodeId));
					} catch (SocketException e) {
						Log.i("In handleFailure", "Multiple failure.. failed to connect to successor also.." + nodeId);
					}
				}
			}
			if(msgType.equals(Constants.DELETE_REQUEST)){
				String[] replicators = Constants.REPLICATORS_MAP.get(getNodeId(port));
				String[] contents = msgType.split("-");
				for (String nodeId : replicators) {
					try {
						if(nodeId.equals(myNodeId)){
							db.delete(Constants.mUri.toString(), msgContents[1],null);
							versionMap.remove(msgContents[1]);
						}
						else{
							Log.i("In handleFailure", "To handle coordinator failure, routing the delete request to the coordinator's successor.." + nodeId);
							connectAndGetResponse(message, getPort(nodeId));
						}

					} catch (SocketException e) {
						Log.i("In handleFailure", "Multiple failure.. failed to connect to successor also.." + nodeId);
					}
				}
			}
			if(msgType.equals(Constants.REPLICATE_DATA)){
				// TODO: Can add code for hinted hand-off. Not required for now.
				Log.i("In handleFailure", "Do nothing when replica is not available to accept delete request.");
				return response;
			}
			if(msgType.equals(Constants.DELETE_REPLICATED_DATA)){
				// TODO: Can add code for hinted hand-off. Not required for now.
				Log.i("In handleFailure", "Do nothing when replica is not available to accept delete request.");
				return response;
			}
		}
		catch (Exception e){
			Log.e("In Client", "Error occurred while handling socket exception.");
			e.printStackTrace();
		}
		return response;
	}

	private String[] processServerResponse(Socket socket, PrintWriter pw) throws IOException {
		socket.setSoTimeout(Constants.socketTimeoutInMilliSeconds);
		socket.setTcpNoDelay(true);
		BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
		String msg = br.readLine();
		Log.i("Client", "received response from server and closing socket.." + msg);
		pw.flush();
		pw.close();
		socket.close();
		if(msg == null){
			return null;
		}
		return msg.split("-");
	}

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
		@Override
		protected void onPreExecute(){

		}
		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			Log.i("In server","Server started");
			ServerSocket serverSocket = sockets[0];
			Log.i("In server","message received by server - " + serverSocket.getLocalPort());
			Socket socket = null;
			try {
				while(true){
					socket =  serverSocket.accept();
					Log.i("In server","Trying to acquire lock at server");
					//synchronized (globalSyncObj){
						BufferedReader br = new BufferedReader( new InputStreamReader(socket.getInputStream()));
						String msg = br.readLine();
						Log.i("In server","Received msg at server is " + msg);
						String[] msgContents = msg.split("-");
						String msgType = msgContents[0];
						if(msgType.equals(Constants.INSERT_REQ_TO_COORDINATOR)
								|| msgType.equals(Constants.REPLICATE_DATA)
								|| msgType.equals(Constants.INSERT_ON_COORDINATOR_FAILURE)){
							synchronized (globalSyncObj) {
								String key = msgContents[1];
								String value = msgContents[2];
								Log.i("In server","Received insert msg of type " + msgType+ " for key " + key + " value is " + value);
								db.insertOrUpdateData(key, value);
								addOrUpdateVersionMap(key);
								if(msgType.equals(Constants.INSERT_REQ_TO_COORDINATOR)){
									replicateData(myNodeId, key, value);
								}
								if(msgType.equals(Constants.INSERT_ON_COORDINATOR_FAILURE)){
									String coordinatorPort = msgContents[3];
									replicateData(getNodeId(coordinatorPort), key, value);
								}
								respondToClient(socket, Constants.SOCKET_CLOSE_ACK);
							}
						}
						if(msgType.equals(Constants.GET_ON_COORDINATOR_FAILURE)
								|| msgType.equals(Constants.GET_FROM_REPLICA)
								|| msgType.equals(Constants.GET_REQUEST)){
							Log.i("In server","Got get request with parameter " + msgContents[1]);
							Cursor cursor;
							synchronized (globalSyncObj) {
								if (msgContents[1].equals(Constants.LOCAL_QUERY)) {
									cursor = db.getAllValues();
								} else {
									cursor = db.getValue(Constants.mUri.toString(), null, msgContents[1], null, null, null, null);
								}
								String response = cursor == null ? "" : getStringFromCursor(cursor, true);
								respondToClient(socket, Constants.GET_REQUEST_RESP + "-" + response);
							}
						}
						if(msgType.equals(Constants.DELETE_REPLICATED_DATA) || msgType.equals(Constants.DELETE_REQUEST)){
							Log.i("In server","Got delete req for replicated data with parameter " + msgContents[1]);
							synchronized (globalSyncObj) {
								if (msgContents[1].equals(Constants.LOCAL_QUERY)) {
									db.deleteAllInDB();
									versionMap = new HashMap<String, Integer>();
								} else {
									db.delete(Constants.mUri.toString(), msgContents[1], null);
									versionMap.remove(msgContents[1]);
								}
								respondToClient(socket, Constants.SOCKET_CLOSE_ACK);
							}
						}
					//}
				}
			} catch (IOException e) {
				e.printStackTrace();
			} catch (Exception e){
				e.printStackTrace();
			} finally {
				try {
					Log.i("In Server:", "Closing the socket");
					socket.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			return null;
		}

		private void respondToClient(Socket socket, String msg) throws IOException {
			DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
			PrintWriter pw = new PrintWriter(dataOutputStream, true);
			pw.println(msg);
		}

		protected void onProgressUpdate(String...strings) {
			return;
		}
	}

	private void replicateData(String nodeId, String key, String value) {
		String[] replicators = Constants.REPLICATORS_MAP.get(nodeId);
		Log.i("replicateData", "Inside replicate data..");
		for(String replicator : replicators){
			if(!replicator.equals(this.myNodeId)){
				try{
					Log.i("replicateData","Contacting the successor..." + replicator + " to replicate key " + key);
					String msgToReplicator = Constants.REPLICATE_DATA + "-" + key + "-" + value;
					connectAndGetResponse(msgToReplicator, getPort(replicator));
				}
				catch (SocketException e){
					Log.i("replicateData","Failed to replicate to successor..." + replicator + " for key " + key);
					e.printStackTrace();
				}
				catch (IOException e){
					Log.i("replicateData","Failed to replicate to successor..." + replicator + " for key " + key);
					e.printStackTrace();
				}
			}
		}
	}

	private void deleteDataInReplicas(String nodeId, String key) {
		String[] replicators = Constants.REPLICATORS_MAP.get(nodeId);
		String msgToReplicator1 = Constants.DELETE_REPLICATED_DATA + "-" +  getPort(replicators[0]) + "-" + key;
		sendMessageThroughClientTask(msgToReplicator1);
		Log.i("replicateData", "Started client task to send to replicator " + replicators[0]);
		String msgToReplicator2 = Constants.DELETE_REPLICATED_DATA + "-" +  getPort(replicators[1])+ "-" + key;
		sendMessageThroughClientTask(msgToReplicator2);
		Log.i("replicateData", "Started client task to send to replicator " + replicators[1]);
	}

	private String genHash(String input) {
		Formatter formatter = new Formatter();
		try {
			MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
			byte[] sha1Hash = sha1.digest(input.getBytes());
			for (byte b : sha1Hash) {
				formatter.format("%02x", b);
			}
		}
		catch (NoSuchAlgorithmException e){
		}
		return formatter.toString();
	}
}
