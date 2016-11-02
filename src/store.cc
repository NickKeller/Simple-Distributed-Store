#include "threadpool.h"
#include "store.grpc.pb.h"
#include "vendor.grpc.pb.h"
#include <fstream>
#include <iostream>
#include <vector>
#include <thread>
#include <grpc++/grpc++.h>

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

class StoreImpl final {
public:
	StoreImpl(std::vector<std::string> ip_addrs) : ip_addresses_(ip_addrs){}

	void run_store(){
		//listen on localhost:55555
		std::string server_address("localhost:12345");

	    ServerBuilder builder;
	    // Listen on the given address without any authentication mechanism.
	    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
	    // Register "service_" as the instance through which we'll communicate with
	    // clients. In this case it corresponds to an *asynchronous* service.
	    builder.RegisterService(&service_);
	    // Get hold of the completion queue used for the asynchronous communication
	    // with the gRPC runtime.
	    cq_ = builder.AddCompletionQueue();
	    // Finally assemble the server
	    server_ = builder.BuildAndStart();
	    std::cout << "Server listening on " << server_address << std::endl;
		handleClientRequests();
	}

private:
	std::vector<std::string> ip_addresses_;
	std::unique_ptr<ServerCompletionQueue> cq_;
  	Store::AsyncService service_;
  	std::unique_ptr<Server> server_;

	////////////////////////////////////////////////////////////////////////////////
	//Useful Methods for StoreImpl
	///////////////////////////////////////////////////////////////////////////////
	void handleClientRequests(){
		//Spawn a new CallData instance to serve new clients.
		new CallData(&service_, cq_.get(), ip_addresses_);
		void* tag;  // uniquely identifies a request.
		bool ok;
		while (true) {
		  std::cout << "Waiting for request" << std::endl;
		  GPR_ASSERT(cq_->Next(&tag, &ok));
		  std::cout << "Asserting okay" << std::endl;
		  GPR_ASSERT(ok);
		  std::cout << "Call" << std::endl;
		  static_cast<CallData*>(tag)->Proceed();
		}
	}


	////////////////////////////////////////////////////////////////////////////////
	//Helper Classes
	///////////////////////////////////////////////////////////////////////////////

	// Class encompasing the state and logic needed to serve a request.
    class CallData {
    public:
		// Take in the "service" instance (in this case representing an asynchronous
		// server) and the completion queue "cq" used for asynchronous communication
		// with the gRPC runtime.
		CallData(Store::AsyncService* service, ServerCompletionQueue* cq, std::vector<std::string> ip_addrs)
		  : service_(service), scq_(cq), responder_(&ctx_), status_(CREATE), vendor_addresses(ip_addrs) {
		// Invoke the serving logic right away.
		Proceed();
		}

		void Proceed() {
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
				// Spawn a new CallData instance to serve new clients while we process
				// the one for this CallData. The instance will deallocate itself as
				// part of its FINISH state.
				new CallData(service_, scq_, vendor_addresses);

				// The actual processing.
				std::string name = request_.product_name();
				std::cout << "Asking for product " << name;
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
				// Once in the FINISH state, deallocate ourselves (CallData).
				delete this;
			}
    	}

	private:
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
	  	explicit VendorClient(std::shared_ptr<Channel> channel, std::string ip_addr)
	      : stub_(Vendor::NewStub(channel)), vendor_address(ip_addr) {}

	  	ProductInfo getProductBid(const std::string& name) {
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
		    } else {
		      std::cout << "RPC failed" << std::endl;
		    }
	  	}
	private:
		// Out of the passed in Channel comes the stub, stored here, our view of the
		// server's exposed services.
		std::unique_ptr<Vendor::Stub> stub_;
		std::string vendor_address;
	};

};

int main(int argc, char** argv) {
	//first, grab the list of vendor addresses
	int num_threads = 1;
    std::string filename;
    if (argc == 3) {
      filename = std::string(argv[1]);
	  num_threads = std::max(1,atoi(argv[2]));
    }
	else if (argc == 2) {
      filename = std::string(argv[1]);
    }
    else {
      std::cerr << "Correct usage: ./store $file_path_for_server_addrress [$num_threads]" << std::endl;
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

    StoreImpl store(ip_addrresses);
	store.run_store();
    return EXIT_SUCCESS;
}
