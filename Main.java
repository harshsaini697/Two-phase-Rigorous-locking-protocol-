package Rigorous2PL;

import java.io.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import Rigorous2PL.Transactions;
import Rigorous2PL.lock;
public class Main {
static String line1;
static Map<Integer,Transactions> transactionTable = new HashMap<Integer,Transactions>(); //Transaction table
static Map<String,lock> lockTable= new HashMap<String,lock>();//lock table

public static void main(String[] args) throws NullPointerException {
	String filename="/Users/harshsaini/Desktop/DB2/Rigorous2PL/src/Rigorous2PL/input7.txt";
	int timestamp=0;
	String line1=null;
	
	try {
		FileReader file = new FileReader(filename);
		BufferedReader read=new BufferedReader(file);
		//line1=read.readLine();
		 while ((line1=read.readLine())!=null){ {
			 String line = line1.replace(" ", "");
			if(line.charAt(0)=='b') {
				timestamp=timestamp+1;
				begin_transaction(timestamp,Integer.parseInt(line.substring(1,line.indexOf(';'))));
				//System.out.print("Transaction "+timestamp);
			}
			else if(line.charAt(0)=='r') {
				//System.out.println(line.substring(line.indexOf('(')+1,line.indexOf(')')));
				request_dataItem(line.substring(line.indexOf('(')+1,line.indexOf(')')),Integer.parseInt(line.substring(1, line.indexOf('('))),line.charAt(0)+"");
			}
			else if(line.charAt(0)=='w') {
				
				request_dataItem(line.substring(line.indexOf('(')+1,line.indexOf(')')),Integer.parseInt(line.substring(1, line.indexOf('('))),line.charAt(0)+"");
				}
			else if(line.charAt(0)=='e') {
				//request_dataItem(line.substring(line.indexOf('(')+1,line.indexOf(')')),Integer.parseInt(line.substring(1, line.indexOf('('))),line.charAt(0)+"");
				commit_transaction(transactionTable.get(Integer.parseInt(line.substring(1, line.indexOf(";")))));
			}
		
		 }}
		read.close();
		  }
	catch(FileNotFoundException e) {
		e.printStackTrace();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
}

	public static void begin_transaction (int timestamp1, int transaction_id){
		Transactions transaction1=new Transactions(transaction_id,timestamp1,"Active");
		transactionTable.put(transaction_id, transaction1);
		System.out.println("Begin operation for Transaction "+transaction_id+" with timestamp "+timestamp1);
		}
	
	public static void request_dataItem(String dataItem, int transaction_id, String operation) {
		//public static void request(String dataItem, int transID, String op){
			operation = operation.equals("r") ? "Read" : "Write";
			Transactions t2 = transactionTable.get(transaction_id); // Incoming ID
			//System.out.println(t2.state); 
			 //state of the transaction is active then
			 if(t2.state.equals("Active")){
				 //System.out.println("lol12"+dataItem+t2.trans_id+t2.dataitems);
				 active_transaction(dataItem,t2, operation);
			 } 
			 //state of the transaction is blocked
			 else if(t2.state.equals("Block")){
				 blocked_transaction(dataItem, t2, operation);
			 }
			 //state of the transaction is aborted
			 else if(t2.state.equals("Abort")){
				 
				 System.out.println("Transaction "+t2.trans_id+" is aborted");			 
			 } 
			 //state of the transaction is committed
			 else if(t2.state.equals("Commit")){
				 System.out.println(" Transaction "+t2.trans_id+" is committed");
			 }
		}
	
	
	//}
	public static void blocked_transaction(String dataItem, Transactions transaction2, String operation) {
		// TODO Auto-generated method stub
		lock lock1 = lockTable.get(dataItem);
		 if(lockTable.containsKey(dataItem)){
			 	
			 lock1.waitinglist.add(transaction2.trans_id);
			 lockTable.put(dataItem,lock1); 
		 } 
		 if(!transaction2.dataitems.contains(dataItem))
			 transaction2.dataitems.add(dataItem);
		 transaction2.waitingOperation.add(new Operation(operation,dataItem));
		lock1.Lockstate="Write";
		 //lock1.waitinglist.add(transaction2.trans_id);
		 transactionTable.put(transaction2.trans_id,transaction2); 
		 transaction2.state="Block";
		
		 System.out.println("Transaction "+transaction2.trans_id+" is in blocked state "+operation+" operation on dataitem "
				 +dataItem+" has been added to the waiting operation queue of transaction table and the transactio ID"
				 		+transaction2.trans_id+" has been added to the waiting list queue of lock table.");
		// releaseLock(transaction2,dataItem);
	}

	
	

	public static void active_transaction(String dataItem, Transactions transaction2, String operation) {
		if(lockTable.containsKey(dataItem)) {
			lock lock1=lockTable.get(dataItem);
		//	String lock_type=lock1.Lockstate;
			//System.out.println("locktype"+lock1.Lockstate);
			if(lock1.Lockstate.equals("Read") && operation.equals("Read"))
				{lock1=read_read(dataItem,transaction2,lock1);}
			else if(lock1.Lockstate.equals("Read") && operation.equals("Write"))
				{lock1=read_write(dataItem,transaction2,lock1);}
			else if(lock1.Lockstate.equals("Write") && operation.equals("Read"))
				{//System.out.println(dataItem+" "+transaction2.state
				//		+" "+lock1.writeTransactionId+" ");
				lock1=write_read(dataItem,transaction2,lock1);}
			else if(lock1.Lockstate.equals("Write") && operation.equals("Write"))
				{lock1=write_write(dataItem,transaction2,lock1);}
			else if(lock1.Lockstate.equals("") && operation.equals("Read")){
				 lock1.Lockstate = "Read";
				 lock1.readTransactionId.add(transaction2.trans_id);
				 System.out.println("Transaction "+transaction2.trans_id+" has acquired Read Lock on data item "+dataItem);
				 }
			else if(lock1.Lockstate.equals("") && operation.equals("Write")){
				 lock1.Lockstate = "Write";
				 System.out.println("Transaction "+transaction2.trans_id+" has acquired Write Lock on data item "+dataItem);
				 lock1.writeTransactionId = transaction2.trans_id;
				 System.out.println(lock1.writeTransactionId);
			 }
				
			
			
			 lockTable.put(dataItem,lock1);
		}
		else {
		//lock lock1 = new lock;
		lock lock1=null;
			if(operation.equals("Read")) {
				lock1=new lock(dataItem,operation,0);
				lock1.readTransactionId.add(transaction2.trans_id);
				System.out.println("Transaction "+transaction2.trans_id+" is Active and will lock data item "+dataItem+". Transaction"+transaction2.trans_id+" has acquired "
				 				+ "shared READ lock on it.");
				
				
			}
			if(operation.equals("Write")) {
				lock1=new lock(dataItem,operation,0);
				lock1.writeTransactionId=transaction2.trans_id;
				System.out.println("Transaction "+transaction2.trans_id+" is Active and will lock data item "+dataItem+". Transaction "+transaction2.trans_id+" has acquired "
				 				+ "Exclusive WRITE Lock on it."+"");
				
				
			}
			if(transaction2.dataitems.contains(dataItem)) {
				transaction2.dataitems.add(dataItem);
				}
			
			transactionTable.put(transaction2.trans_id, transaction2);
			lockTable.put(dataItem, lock1);
			
	}
		}
	
	
	
	private static lock write_write(String dataItem, Transactions transaction2, lock lock1) {
		// TODO Auto-generated method stub
		
		 Transactions t1 = transactionTable.get(lock1.writeTransactionId);
		/* if(t1.state=="Abort"){
				//System.out.println("Exclusive lock on "+dataItem+" by Transaction "+t3.trans_id+" will be converted to "+ transaction2.trans_id+"Shared lock.");
				lock1.Lockstate="Write";
				//lock1.writeTransactionId=0;
				//lock1.readTransactionId.add(transaction2.trans_id);
				if(!transaction2.dataitems.contains(dataItem))
				transaction2.dataitems.add(dataItem);
				
				lock1.Lockstate="Write";
				// lock1.readTransactionId.remove(0);
				 lock1.writeTransactionId = transaction2.trans_id;
				 System.out.println("For the data item "+dataItem+" and transaction ID "+transaction2.trans_id+"  has been Write Lock."+"");
		 }*/
		 if(t1.timestamp>transaction2.timestamp){
			 t1.state="Block";
			 
			 transactionTable.put(t1.trans_id, t1);
			 lock1.writeTransactionId=transaction2.trans_id;
			 System.out.println("Transaction "+transaction2.trans_id+" is blocked because of timestamp and transaction");
			 blocked_transaction(dataItem,transaction2,"Write");
			 releaseLock(t1,dataItem);
		 }
		 else{
			 transaction2.state = "Abort";
			
			 System.out.println("Write operation for "+dataItem+" by Transaction "+transaction2.trans_id+" has been aborted because of high timestamp. ");
			 transactionTable.put(transaction2.trans_id,transaction2);
			// lock1.writeTransactionId=transaction2.trans_id;
			// lock1.Lockstate="W";
			//lock1.readTransactionId.add(transaction2.trans_id);
			 releaseLock(transaction2,dataItem);
			 
			 
		 }
		 if(!transaction2.dataitems.contains(dataItem)){
			 transaction2.dataitems.add(dataItem);
			 transactionTable.put(transaction2.trans_id, transaction2);
		 }
			 
		 return lock1;
	}

	private static lock write_read(String dataItem, Transactions transaction2, lock lock1) {
		// TODO Auto-generated method stub
		//for the same transaction to denegrate the lock
		Transactions t3=transactionTable.get(lock1.writeTransactionId);
		if(lock1.writeTransactionId == transaction2.trans_id) {
			lock1.Lockstate="Read";
			lock1.writeTransactionId=0;
			lock1.readTransactionId.add(transaction2.trans_id);
			if(!transaction2.dataitems.contains(dataItem))
			transaction2.dataitems.add(dataItem);
			
			transactionTable.put(transaction2.trans_id, transaction2);
			System.out.println("Exclusive lock on "+dataItem+"by Transaction"+transaction2.trans_id+" will be converted to Shared lock.");
			
			
		}
		
		
		else {
				//Transactions t3=transactionTable.get(lock1.writeTransactionId);// getting transaction who has the write lock
				if(t3.timestamp>transaction2.timestamp) {
					transaction2.state="Block";
					blocked_transaction(dataItem,transaction2,"Read");
					//transaction2.waitingOperation.add(new Operation("Read",dataItem));
				//	lock1.waitinglist.add(transaction2.trans_id);
					//transactionTable.put(transaction2.trans_id, transaction2);
					//if(!transaction2.dataitems.contains(dataItem))
					//	 transaction2.dataitems.add(dataItem);
					System.out.println("Transaction "+transaction2.trans_id+" has been blocked(waiting) since Transaction"+transaction2.timestamp+" is a older transaction."
							+ "write lock would remain with"+t3.trans_id+"until it frees the data item");
					releaseLock(t3,dataItem);
							
				}	
				else {	
				//// inserting values in the locktable.
					transaction2.state="Abort";
					 transactionTable.put(transaction2.trans_id,transaction2);
					 lock1.writeTransactionId=0;
					 lock1.Lockstate="Read";
					 lock1.readTransactionId.add(transaction2.trans_id);
					 
					 //transactionTable.put(transaction2.trans_id, transaction2);
					 System.out.println("Transaction "+transaction2.trans_id+" aborts because it has higher timestamp and transaction "
					 +t3.trans_id+" will keep the write lock on "+dataItem+"");
					abort(transaction2);
					
				}
			
			
		}
		return lock1;
		}
	
	

	private static lock read_write(String dataItem, Transactions transaction2, lock lock1) {
		// TODO Auto-generated method stub
		 Transactions t3 = transactionTable.get(lock1.writeTransactionId);
		
		 if(lock1.readTransactionId.size()==1 && lock1.readTransactionId.get(0).equals(transaction2.trans_id)){
			lock1.Lockstate="Write";
			 lock1.readTransactionId.remove(0);
			 lock1.writeTransactionId = transaction2.trans_id;
			 System.out.println("For the data item "+dataItem+" and transaction ID "+transaction2.trans_id+" lock has been upgraded to Write Lock."+"");
		 }
		else if(lock1.readTransactionId.size()==1 && !lock1.readTransactionId.get(0).equals(transaction2.trans_id)){
			 //Transactions t3 = transactionTable.get(lock1.writeTransactionId);//
			 
			 if(t3.timestamp>transaction2.timestamp) {
					transaction2.state="Block";
					//lock1.Lockstate="Write";
					//lock1.writeTransactionId=transaction2.trans_id;
					//transaction2.waitingOperation.add(new Operation("Write",dataItem));
					//lock1.waitinglist.add(transaction2.trans_id);
					blocked_transaction(dataItem,transaction2,"Write");
					transactionTable.put(transaction2.trans_id, transaction2);
					System.out.println("Transaction"+transaction2.trans_id+"has been blocked(waiting) since Transaction"+transaction2.timestamp+" is a older transaction."
							+ "Read lock would remain with"+t3.trans_id+"until it frees the data item");
					releaseLock(t3,dataItem);
							
				}	
			 else {	
						transaction2.state="Abort";
						 transactionTable.put(transaction2.trans_id,transaction2);
						
						 //transactionTable.put(transaction2.trans_id, transaction2);
						 
						 System.out.println("Transaction "+transaction2.trans_id+" aborts because it has higher timestamp and transaction "
						 +t3.trans_id+" keep the write lock on "+dataItem+"\n");
						 abort(transaction2);
						// releaseLock(t3,dataItem);
						
						
			 }
			 }
		else if(lock1.readTransactionId.size()>1){
				 List<Integer> readTransId = lock1.readTransactionId;
				 Collections.sort(readTransId);
				 int first = readTransId.get(0);
				 if(first == transaction2.trans_id){
					System.out.println("Transaction "+first+" will be blocked for write operation until Transaction+"+readTransId.get(1)+"commits");
					blocked_transaction(dataItem,transaction2,"Write");
					 // System.out.println("For transaction "+transaction2.trans_id+" lock for data item "+dataItem+" has been upgraded to write.");
					/* for(int i = 1;i<readTransId.size();i++){
							t3 = transactionTable.get(readTransId.get(i));
					//		System.out.println("lol");
							abort(t3);
							System.out.println("Aborting transaction "+t3.trans_id);
						 }*/
					 
	
					 lock1.readTransactionId.clear();
					 lock1.writeTransactionId = first;
				 } else if(transaction2.trans_id < first){
					 System.out.println("Transaction "+transaction2.trans_id+" has acquired write lock for data item "+dataItem);

					 for(int i = 0;i<readTransId.size();i++){
							Transactions t1 = transactionTable.get(readTransId.get(i));
							abort(t1);
							System.out.println("Aborting transaction "+t1.trans_id);
						 }
					 lock1.readTransactionId.clear();
					 lock1.writeTransactionId= first;
				 } else {
					 transaction2.state = "Abort";
					 transactionTable.put(transaction2.trans_id, transaction2);

					 //transaction2.waitingOperation.add(new Operation(dataItem, "Write"));
					 System.out.println("Transaction "+transaction2.trans_id+" has been aborted because it has higher timestamp.");
					 //abort(transaction2);
					 int i = 0;
					 while(i<readTransId.size()){
						// System.out.println("in"+"i is"+i+"and"+readTransId.size() +"");
						    if(transaction2.trans_id <=readTransId.get(i)) {i++;
						    blocked_transaction(dataItem,transaction2,"Write");break;}
						    if(transaction2.trans_id >readTransId.get(i)) {
						  //  	System.out.println("inner");
						   	readTransId.remove(i);
						    	Transactions t1 = transactionTable.get(readTransId.get(i));
					//	    	System.out.print("read trans id"+t1.trans_id);
						    	abort(transaction2);
						//	System.out.println("Aborting transaction lol "+t1.trans_id);
						
							++i;
						    }	
					  }	 
					 lock1.readTransactionId = readTransId;
				 }

					 
				 
			 }
			if(!transaction2.dataitems.contains(dataItem)){
			 transaction2.dataitems.add(dataItem);
			 transactionTable.put(transaction2.trans_id, transaction2);
			}
			 return lock1;
		}

	private static void abort(Transactions t1) {
		// TODO Auto-generated method stub
		 t1.state = "Abort";
		 Queue<String>  DataItems = t1.dataitems;
		 
			System.out.println("Releasing locks acquired by transaction "+t1.trans_id);
			while(!DataItems.isEmpty()){
				String d = DataItems.remove();
				releaseLock(t1, d);
		 }
		 transactionTable.put(t1.trans_id, t1);
		 //System.out.println("abort done");
	}

	private static lock read_read(String dataItem, Transactions transaction2, lock lock1) {
		// TODO Auto-generated method stub
		 lock1.readTransactionId.add(transaction2.trans_id);
		// System.out.println("read trans in read read"+lock1.readTransactionId);
		 if(!transaction2.dataitems.contains(dataItem))
		 transaction2.dataitems.add(dataItem);
		// System.out.println("data itmes in read read"+transaction2.dataitems);
		 transactionTable.put(transaction2.trans_id, transaction2);
		 System.out.println("Transaction "+transaction2.trans_id+" also has shared READ lock on "
		 		+dataItem+ ".");

		
		return lock1;
	}
	

	public static void commit_transaction(Transactions in) {
		//System.out.print("transaction.state:"+in.state);
		// TODO Auto-generated method stub
			if(in.state.equals("Active")){
				System.out.println("Committing Transaction "+in.trans_id+".");
				in.state ="Commit";
				Queue<String>  DataItems = in.dataitems;
				
				System.out.println("Releasing locks of"+in.trans_id);
				while(!DataItems.isEmpty()){
					//System.out.println("Releasing locks inner loop");
					String d = DataItems.remove();
					releaseLock(in, d);
				}
				//System.out.println("Transaction "+in.trans_id+" has been committed and locks have been released\n");
				
				//lock.readTransactionId.remove(in.transId);
				//bring the priority queue element and change the state to of that transaction to active
			}
			else if(in.state.equals("Block")){
				 in.waitingOperation.add(new Operation("Commit", ""));
				 transactionTable.put(in.trans_id, in);	
				 System.out.println("Commit operation on transaction "+in.trans_id+" has been added to the waiting operation");
				
			} 
			else if (in.state.equals("Abort")){
				System.out.println("Transaction "+in.trans_id+" cannot be committed because it has already been aborted.");
			}
		}

	private static void releaseLock(Transactions in, String dataitem) {
		// TODO Auto-generated method stub
		lock lock1 = lockTable.get(dataitem);
		//System.out.println("read trans"+lock1.readTransactionId+in.dataitems);
		
		
		if(lock1.Lockstate.equals("Write") || lock1.readTransactionId.size()==1){
			Queue<Integer> wt= lock1.waitinglist;
			//System.out.println("wt"+lock1.waitinglist);
			lock1.Lockstate = "";
			if(lock1.readTransactionId.size()==1){
				lock1.readTransactionId.remove(0);
				System.out.println("Transaction "+in.trans_id+" has released read lock on "+dataitem);
			}else{
				System.out.println("Transaction "+in.trans_id+" has released write lock on "+dataitem);
			}
			lockTable.put(dataitem, lock1);
			if(wt.isEmpty()){
				lockTable.remove(dataitem);
				
			} 
			
			else {
				//System.out.println("Wt"+lock1.waitinglist);
				while(!lock1.waitinglist.isEmpty()){
					int tid = lock1.waitinglist.remove();
					Transactions t3 = transactionTable.get(tid);
					
					t3= acquireLocks(t3, dataitem, lock1);
					transactionTable.put(tid, t3);
					if(!t3.state.equals("Commit")){
						return;
					}
				}
			}
			
			lockTable.remove(dataitem);	
		}
		else if(lock1.Lockstate.equals("Read")){
			List<Integer> rtids = lock1.readTransactionId;
			for(int i = 0; i < rtids.size(); ++i ){
				if(rtids.get(i) == in.trans_id){
					rtids.remove(i);
				}
			}
			System.out.println("Transaction "+in.trans_id+" has released read lock on "+dataitem);
			System.out.println("dekj "+lock1.readTransactionId);
			lockTable.put(dataitem, lock1);
			//System.out.println("Done");
		}
	}

	private static Transactions acquireLocks(Transactions t3, String dataitem, lock lock1) {
		// TODO Auto-generated method stub
	
			Queue<Operation> waitingOperations = t3.waitingOperation;
			t3.state="Active";//
			transactionTable.put(t3.trans_id,t3);//
			
			
			if(!waitingOperations.isEmpty()){
				System.out.println("Transaction "+t3.trans_id+" has been changed from Block to Active");
				System.out.println("Running its waiting operations");
			}
			while(!waitingOperations.isEmpty()){
				Operation o = waitingOperations.remove();
				if(o.operation.equals("Read")){
					request_dataItem(o.dataItem, t3.trans_id,"r");
				} else if(o.operation.equals("Write")){
					request_dataItem(o.dataItem, t3.trans_id,"w");
				} else if(o.operation.equals("Commit")){
					commit_transaction(t3);
					
				}
			
				
			}
			
			lockTable.put(dataitem, lock1);
			
			return t3;
		}

	
	
	
	
	



}
