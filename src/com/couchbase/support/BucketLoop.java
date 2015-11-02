package com.couchbase.support;

import java.net.URI;
import java.util.ArrayList;

import com.couchbase.client.ClusterManager;
import com.couchbase.client.clustermanager.BucketType;


// This uses Couchbase Java SDK 1.4.9
// Perform bucket operations ( create, delete ) in loop
// Brian Williams
// November 2, 2015

public class BucketLoop {

	public static void main(String[] args) {

		System.out.println("Welcome to BucketLoop");

		ArrayList<URI> nodes = new ArrayList<URI>();
		nodes.add(URI.create("http://192.168.38.101:8091/pools"));
		
		String username = "Administrator";
		String password = "couchbase";
		
		ClusterManager cm = new ClusterManager(nodes, username, password);
				
		String bucketName = "testBucket20151102";
		
		BucketType bucketType = BucketType.COUCHBASE;
		int memorySizeMB = 500;
		int replicas = 1;
		String authPassword = "password";
		boolean flushEnabled = true;
		
		boolean keepGoing = true;
		
		long t1 = 0, t2 = 0;
		
		int createBucketExceptionCount = 0;
		int deleteBucketExceptionCount = 0;
		
		long timeToCreateBucket = 0;
		long timeToDeleteBucket = 0;
		
		String exceptionClassName;
		
		int iteration = 0;
		
		while (keepGoing) {
			
			// Create bucket
			t1 = System.currentTimeMillis();
			try {
				System.out.println("About to create bucket");
				cm.createNamedBucket(bucketType, 
						bucketName, 
						memorySizeMB, 
						replicas, 
						authPassword, 
						flushEnabled);
				System.out.println("Done with create bucket");
			}
			catch (Exception e) {
				createBucketExceptionCount++;
				exceptionClassName = e.getClass().getCanonicalName();
				System.out.println("Caught " + exceptionClassName + " :" + e);
				e.printStackTrace();
				
				if (exceptionClassName.equals("Bucket with given name already exists")) {
					System.out.println("The bucket already exists");
				}
				else {
					keepGoing = false;
				}
				
			}
			t2 = System.currentTimeMillis();
			timeToCreateBucket = t2 - t1;
			
			// Delete bucket
			t1 = System.currentTimeMillis();
			try {
				System.out.println("About to delete bucket");
				cm.deleteBucket(bucketName);
				System.out.println("Done with delete bucket");

			}
			catch (Exception e) {
				deleteBucketExceptionCount++;
				exceptionClassName = e.getClass().getCanonicalName();
				System.out.println("Caught " + exceptionClassName + " :" + e);
				e.printStackTrace();
				keepGoing = false;				
			}
				
			t2 = System.currentTimeMillis();
			timeToDeleteBucket = t2 - t1;
			
			// Done with items
			
			iteration++;
			
			System.out.println("Iteration:" + iteration + " Time to create bucket:" + timeToCreateBucket + " Time to delete bucket:" + timeToDeleteBucket);
			System.out.println("            Exceptions on create: " + createBucketExceptionCount + " on delete: " + deleteBucketExceptionCount);
			
		} // main loop
	
		System.out.println("Now leaving BucketLoop.  Goodbye.");
	
	}

	
	
}
