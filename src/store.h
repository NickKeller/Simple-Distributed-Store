#pragma once
#include <iostream>
#include <deque>
#include <functional>
#include <thread>
#include <condition_variable>
#include <mutex>
#include <random>
#include <climits>
#include <fstream>
#include <vector>
#include <climits>
#include <grpc++/grpc++.h>
//#include "threadpool.h"
#include "store.grpc.pb.h"
#include "vendor.grpc.pb.h"

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ClientAsyncResponseReader;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ClientContext;
using grpc::Channel;
using grpc::ServerCompletionQueue;
using grpc::CompletionQueue;
using grpc::Status;
using store::ProductQuery;
using store::ProductReply;
using store::ProductInfo;
using store::Store;
using vendor::Vendor;
using vendor::BidQuery;
using vendor::BidReply;


////////////////////////////////////////////////////////////////////////////////
//Helper Classes
///////////////////////////////////////////////////////////////////////////////

// Class encompasing the state and logic needed to serve a request.
class CallData {
public:
	CallData(Store::AsyncService* service, ServerCompletionQueue* cq, std::vector<std::string> ip_addrs);
	CallData();
	void Proceed();

private:
	int tag_count;
	std::vector<std::string> vendor_addresses;
	Store::AsyncService* service_;
	ServerCompletionQueue* scq_;
	ServerContext ctx_;
	ProductQuery request_;
	ProductReply reply_;
	ServerAsyncResponseWriter<ProductReply> responder_;
	enum CallStatus { CREATE, PROCESS, FINISH };
	CallStatus status_;  // The current serving state.
};

//class used to connect to a specific vendor
class VendorClient {
	public:
  	explicit VendorClient(std::shared_ptr<Channel> channel, std::string ip_addr);
  	ProductInfo getProductBid(const std::string& name);

private:
	std::unique_ptr<Vendor::Stub> stub_;
	std::string vendor_address;
};

class ThreadPool {
public:

	ThreadPool();

	ThreadPool(unsigned int num_threads);

	~ThreadPool();

	//template<class F>
	// void enqueue(F&& f);

	void enqueue(CallData* data);


private:
	std::vector<std::thread> workers;
	//std::deque<std::function<void()>> tasks;
	std::deque<CallData*> tasks;
	std::mutex queue_mutex;
    std::condition_variable cv_task;
    std::condition_variable cv_finished;
    bool stop;

    void thread_proc();
};

class StoreImpl final {
public:
	StoreImpl(std::string bindAddr, std::vector<std::string> ip_addrs, int num_threads);
	void run_store();
	~StoreImpl();
private:
	std::string bindAddress;
	std::vector<std::string> ip_addresses_;
	int numThreads;
	std::unique_ptr<ServerCompletionQueue> cq_;
	Store::AsyncService service_;
	std::unique_ptr<Server> server_;
	ThreadPool tp;
	void handleClientRequests();
	std::deque<CallData*> dataQ;
};
