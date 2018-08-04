package Rigorous2PL;


import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

public class lock {
String dataitem;
String Lockstate;
int writeTransactionId;
List<Integer> readTransactionId;
PriorityQueue<Integer> waitinglist;
public lock(String dataitem, String lockstate, int writeTransactionId) {
	super();
	this.dataitem = dataitem;
	Lockstate = lockstate;
	this.writeTransactionId = writeTransactionId;
	readTransactionId= new ArrayList<Integer>();
	waitinglist=new PriorityQueue<Integer>();
	
}
public lock() {
	super();
	readTransactionId=new ArrayList<Integer>();
	waitinglist=new PriorityQueue<Integer>();
	
	
}
public String getDataitem() {
	return dataitem;
}
public void setDataitem(String dataitem) {
	this.dataitem = dataitem;
}
public String getLockstate() {
	return Lockstate;
}
public void setLockstate(String lockstate) {
	Lockstate = lockstate;
}
public int getWriteTransactionId() {
	return writeTransactionId;
}
public void setWriteTransactionId(int writeTransactionId) {
	this.writeTransactionId = writeTransactionId;
}
public List<Integer> getReadTransactionId() {
	return readTransactionId;
}
public void setReadTransactionId(List<Integer> readTransactionId) {
	this.readTransactionId = readTransactionId;
}
public PriorityQueue<Integer> getWaitinglist() {
	return waitinglist;
}
public void setWaitinglist(PriorityQueue<Integer> waitinglist) {
	this.waitinglist = waitinglist;
}


}
