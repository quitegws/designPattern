import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class LineWritable implements Writable {
	
	private long accessCount;
	private String userId;
	private String serverIP;
	private String hostName;
	private String spName;
	private long uploadTraffic;
	private long downloadTraffic;
	
	public LineWritable(){
		super();
	}

	public LineWritable(long accessCount, String userId, String serverIP, String hostName,
						String spName, long uploadTraffic, long downloadTraffic){
		super();
		this.accessCount = accessCount;
		this.userId = userId;
		this.serverIP = serverIP;
		this.hostName = hostName;
		this.spName = spName;
		this.uploadTraffic = uploadTraffic;
		this.downloadTraffic = downloadTraffic;
	}
	public String toString(){
		return accessCount+"\t"+userId+"\t"+serverIP+"\t"+hostName+"\t"+spName+"\t"+uploadTraffic+"\t"+downloadTraffic;
	}
	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		accessCount = arg0.readLong();
		userId = arg0.readUTF();
		serverIP = arg0.readUTF();
		hostName = arg0.readUTF();
		spName = arg0.readUTF();
		uploadTraffic = arg0.readLong();;
		downloadTraffic = arg0.readLong();

	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		arg0.writeLong(accessCount);
		arg0.writeUTF(userId);
		arg0.writeUTF(serverIP);
		arg0.writeUTF(hostName);
		arg0.writeUTF(spName);
		arg0.writeLong(uploadTraffic);
		arg0.writeLong(downloadTraffic);
		
	}

	public void setAccessCount(long accessCount){
		this.accessCount = accessCount;
	}
	
	public long getAccessCount(){
		return this.accessCount;
	}
	
	public String getUserID() {
		return userId;
	}

	public void setUserID(String userID) {
		this.userId = userID;
	}

	public String getServerIP() {
		return serverIP;
	}

	public void setServerIP(String serverIP) {
		this.serverIP = serverIP;
	}

	public String getHostName() {
		return hostName;
	}

	public void setHostName(String hostName) {
		this.hostName = hostName;
	}

	public String getSpName() {
		return spName;
	}

	public void setSpName(String spName) {
		this.spName = spName;
	}
	
	public long getUploadTraffic(){
		return this.uploadTraffic;
	}
	
	public void setUploadTraffic(long uploadTraffic){
		this.uploadTraffic = uploadTraffic;
	}
	
	public long getDownloadTraffic(){
		return this.downloadTraffic;
	}
	
	public void setDownloadTraffic(long downloadTraffic){
		this.downloadTraffic = downloadTraffic;
	}
}
