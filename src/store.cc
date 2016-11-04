#include <fstream>
#include <iostream>
#include <vector>
#include <thread>
#include <climits>
#include <grpc++/grpc++.h>
#include "store.h"
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

StoreImpl::StoreImpl(std::string bindAddr, std::vector<std::string> ip_addrs, int num_threads) : bindAddress(bindAddr), ip_addresses_(ip_addrs), tp(num_threads){}

StoreImpl::~StoreImpl() {
	server_->Shutdown();
	// Always shutdown the completion queue after the server.
	cq_->Shutdown();
}

void StoreImpl::run_store(){
    ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(bindAddress, grpc::InsecureServerCredentials());
    // Register "service_" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *asynchronous* service.
    builder.RegisterService(&service_);
    // Get hold of the completion queue used for the asynchronous communication
    // with the gRPC runtime.
    cq_ = builder.AddCompletionQueue();
    // Finally assemble the server
    server_ = builder.BuildAndStart();
    std::cout << "Server listening on " << bindAddress << std::endl;
	handleClientRequests();
}

CallData::CallData(Store::AsyncService* service, ServerCompletionQueue* cq, std::vector<std::string> ip_addrs)
  : service_(service), scq_(cq), responder_(&ctx_), status_(CREATE), vendor_addresses(ip_addrs) {
	  Proceed();
}

void CallData::Proceed() {
	if (status_ == CREATE) {
		std::cout << "Status CREATE" << std::endl;
		// Make this instance progress to the PROCESS state.
		status_ = PROCESS;

		// As part of the initial CREATE state, we *request* that the system
		// start processing SayHello requests. In this request, "this" acts are
		// the tag uniquely identifying the request (so that different CallData
		// instances can serve different requests concurrently), in this case
		// the memory address of this CallData instance.
		std::cout << "RequestgetProducts" << std::endl;
		service_->RequestgetProducts(&ctx_, &request_, &responder_, scq_, scq_,
		                          this);
	}
	else if (status_ == PROCESS) {
		std::cout << "Status PROCESS" << std::endl;
		// The actual processing.
		std::string name = request_.product_name();
		std::cout << "Asking for product " << name << std::endl;
		//querying all of the vendors
		for(auto const& ip_addr: vendor_addresses){
			std::cout << "Connecting to vendor " << ip_addr << std::endl;
			VendorClient client(grpc::CreateChannel(ip_addr, grpc::InsecureChannelCredentials()),ip_addr);
			ProductInfo bid = (client.getProductBid(name));
			store::ProductInfo* info_test = reply_.add_products();
           	info_test->set_price(bid.price());
           	info_test->set_vendor_id(bid.vendor_id());
		}

		std::cout << "Products Size: " << reply_.products_size() << std::endl;
		//set to finished
		status_ = FINISH;
		responder_.Finish(reply_, Status::OK, this);
	}
	else {
		std::cout << "Status Finish" << std::endl;
		GPR_ASSERT(status_ == FINISH);
		delete this;
	}
}

VendorClient::VendorClient(std::shared_ptr<Channel> channel, std::string ip_addr)
: stub_(Vendor::NewStub(channel)), vendor_address(ip_addr) {}

ProductInfo VendorClient::getProductBid(const std::string& name) {
	// Data we are sending to the server.
	BidQuery request;
	request.set_product_name(name);
	BidReply reply;
	ClientContext context;
	CompletionQueue cq;
	Status status;
	std::cout << "Getting product bid" << std::endl;
	std::unique_ptr<ClientAsyncResponseReader<BidReply> > rpc(
	    stub_->AsyncgetProductBid(&context, request, &cq));
	rpc->Finish(&reply, &status, (void*)1);
	void* got_tag;
	bool ok = false;
	GPR_ASSERT(cq.Next(&got_tag, &ok));
	GPR_ASSERT(got_tag == (void*)1);
	GPR_ASSERT(ok);

	// Act upon the status of the actual RPC.
	if (status.ok()) {
	  std::cout << "okay" << std::endl;
	  std::cout << "Price: " << reply.price() << "\nVendor id: " << reply.vendor_id() << std::endl;
	  ProductInfo info;
	  info.set_price(reply.price());
	  info.set_vendor_id(reply.vendor_id());
	  return info;
	}
	else {
	  std::cout << "RPC failed" << std::endl;
	}
}

void StoreImpl::handleClientRequests(){
	void* tag;  // uniquely identifies a request.
	bool ok;
	int tag_count = 1;
	while (true) {
		new CallData(&service_, cq_.get(), ip_addresses_);
		std::cout << "Waiting for request" << std::endl;
		GPR_ASSERT(cq_->Next(&tag, &ok));
		std::cout << "Asserting okay" << std::endl;
		GPR_ASSERT(ok);
		std::cout << "Call" << std::endl;
		CallData* handler = static_cast<CallData*>(tag);
		tp.enqueue(handler);
	}
}

ThreadPool::ThreadPool(){
	//default is 4 threads
	ThreadPool(4);
}

ThreadPool::ThreadPool(unsigned int n) : stop()
{
    for (unsigned int i=0; i<n; ++i)
        workers.emplace_back(std::bind(&ThreadPool::thread_proc, this));
}

ThreadPool::~ThreadPool()
{
    // set stop-condition
    std::unique_lock<std::mutex> latch(queue_mutex);
    stop = true;
    cv_task.notify_all();
    latch.unlock();

    // all threads terminate, then we're done.
    for (auto& t : workers)
        t.join();
}

void ThreadPool::thread_proc()
{
	while (true)
	{
		std::unique_lock<std::mutex> latch(queue_mutex);
		cv_task.wait(latch, [this](){ return stop || !tasks.empty(); });
		if (!tasks.empty())
		{
			// pull from queue
			auto fn = tasks.front();
			tasks.pop_front();

			// release lock. run async
			latch.unlock();

			// run function outside context
			fn->Proceed();
			// fn();
			cv_finished.notify_one();
		}
		else if (stop)
		{
			break;
		}
	}
}

// generic function push
// template<class F>
// void ThreadPool::enqueue(F&& f)
// {
// 	std::unique_lock<std::mutex> lock(queue_mutex);
// 	tasks.emplace_back(std::forward<F>(f));
// 	cv_task.notify_one();
// }

void ThreadPool::enqueue(CallData* data)
{
    std::unique_lock<std::mutex> lock(queue_mutex);
	tasks.emplace_back(data);
    cv_task.notify_one();
}



int main(int argc, char** argv) {
	//first, grab the list of vendor addresses
	int num_threads = 1;
    std::string filename;
	std::string bindAddress;
    if (argc == 4) {
		bindAddress = std::string(argv[1]);
		filename = std::string(argv[2]);
		num_threads = std::max(1,atoi(argv[3]));
    }
    else {
		std::cerr << "Correct usage: ./store $bind_address $file_path_for_server_addrress [$num_threads]" << std::endl;
		std::cerr << "Example: ./store localhost:12345 vendor_addresses.txt 4" << std::endl;
		return EXIT_FAILURE;
    }

    std::vector<std::string> ip_addrresses;

    std::ifstream myfile (filename);
    if (myfile.is_open()) {
      	std::string ip_addr;
      	while (getline(myfile, ip_addr)) {
        	ip_addrresses.push_back(ip_addr);
    	}
      myfile.close();
    }
    else {
      std::cerr << "Failed to open file " << filename << std::endl;
      return EXIT_FAILURE;
    }

	for (auto const ip_addr: ip_addrresses){
		std::cout << "IP: " << ip_addr << std::endl;
	}

	std::cout << "Using " << num_threads << " threads" << std::endl;

    StoreImpl store(bindAddress,ip_addrresses,num_threads);
	store.run_store();
    return EXIT_SUCCESS;
}
