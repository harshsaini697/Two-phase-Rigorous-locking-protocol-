package Rigorous2PL;
import java.util.*;

public class Transactions {
int trans_id;
int timestamp;
String state;
Queue<String> dataitems;
Queue<Operation> waitingOperation;
public Transactions(int trans_id, int timestamp, String state) {
	super();
	this.trans_id = trans_id;
	this.timestamp = timestamp;
	this.state = state;
	dataitems= new LinkedList<String>();
	waitingOperation = new LinkedList<Operation>();
}
public int getTrans_id() {
	return trans_id;
}
public void setTrans_id(int trans_id) {
	this.trans_id = trans_id;
}
public int getTimestamp() {
	return timestamp;
}
public void setTimestamp(int timestamp) {
	this.timestamp = timestamp;
}
public String getState() {
	return state;
}
public void setState(String state) {
	this.state = state;
}



}
