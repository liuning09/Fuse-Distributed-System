# Lab2 Overview A53042332 Qi Li

## lab2.go:
A new client named clientWithPrefix is created based on client in Lab1. It will call the functions of lab1 client to issue an RPC call. It acts as a proxy that help escape colons and concat a prefix (bin name). This client will be created then Bin() is called. 

The keeper is also in this file. It will send Ready at the beginning. Then, it will repeatedly communicate with all backends using Clock() RPC call. The first round the keep will try to find the biggest logic clock and use this value as the "atLeast" value in the second round. 

## front.go:
This file implements the frontend server. Concurrency problem is hard to deal because of the following issues:
  * Different frontend servers do not talk to each other. 
  * No atomic test-and-set function RPC call provided. So shared lock is hard to implement.
  * Backend side functions are limited. e.g. we have to allow duplicate elements when doing ListAppend and delete all of them by doing ListRemove. This makes it even harder to create other ways to ensure synchronization.

I tried to simulate a "Lock". Every frontend server has its own ID and will set the "Lock" value to its ID value. Release is not necessary which means the server is still stateless. During this "Unlock" we will check is this ID has changed. Since every server has its own lock, it will not overwirte this "Lock". So, we will know if the same memory is touched by other servers. This scheme is applied to Signup operation. 

When I was trying to apply it to Follow/Unfollow...I found out it is not possible because undo cannot be done since ListRemove will remove all the duplicate elements. 

To handle concurrent Follow calls. I will allow duplicate users on the followings list, but I will delelte duplicates in Following call. This might be slow, but it is very effective.

The ListUser call is implement by a global user cache, which will stop growing after user number reaches Max. This should be scalable. (What is the point of listing 20 users? ) 

If you have any question, please drop me an email: liqi@ucsd.edu

# Lab3 Overview A53042332 Qi Li + Mengqi Yu & Ning Liu

System Structure

       |client|     |client|      |client|     |client|


  chord                     chord          chord         
|backend1|   |backend2|   |backend3|    |backend4|    |backend5|    |backend6|
                bbs            bbs          bbs


     |keeper1|      |keeper2|     |keeper3|  
	   b1 b2          b2 b5         b3 b6


Description:
The organization of the backend servers are stored as a distributed hash table similar to Chord. We name it chord table. This table is replicated in backed servers. (eventually I pushed it to all backends to avoid errors. This is also scalable)
Another table named "bbs" (search Bins By Server) table is defined and also replciated on backend servers. Using this table we can findout which bins are on which servers. These two configuration tables are maintained by keepers. 

A keeper is responsible for part of all servers to make it scalable. The keepers host RPC servers by themselves, so they can clock each other. The heartbeat is consisted of two parts, backends heartbeat and keeper heartbeat. Both of them will compare the RPC status with in memory status table. When server / keeper online or offline. We will handle it respectively.


## logger.go
Store log instead of string as value. This helper encode / decode log.

## replica_client.go
Client implementation. A deamon thread will fetch Chord config from backend servers for client. Bin() will use Chord to find the three server addresses to replicate. Other Set/Get/ListGet/... nothing special..

## keeper_2.go
The v2.0 keeper we implemented. Due to code merge and the super long debugging and patching process, it is not human readable. My initial design is not detailed enough so a lot of troubles are caused. Sorry for the inconvenience. So, to make yours and my life easier, I will just let you know my design flaws so you can find out my problems easier. 1) The logging is not garbage collected (set/get/listremove/etc..), so the performance will be bad when data size is big. 2) I tried to optimize client performance by storing chord table on the backend servers.. so looking for chord table is painful. I finally stored chord table on all backend servers... 

