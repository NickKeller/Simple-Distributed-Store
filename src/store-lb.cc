
//#include "threadpool.h"
#include <grpc++/grpc++.h>
#include <iostream>
#include <fstream>
#include "store.grpc.pb.h"
#include "vendor.grpc.pb.h"
//#include "threadpool.h"

using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerCompletionQueue;
using grpc::Status;
using store::ProductReply;
using store::ProductQuery;
using store::Store;
using vendor::Vendor;
using vendor::BidQuery;
using vendor::BidReply;

class CallData {
    public:
        // Take in the "service" instance (in this case representing an asynchronous
        // server) and the completion queue "cq" used for asynchronous communication
        // with the gRPC runtime.
        CallData(Store::AsyncService* service, ServerCompletionQueue* cq,
                std::vector<std::string> vendor_list)
            : service_(service), cq_(cq), responder_(&ctx_), status_(CREATE),
            addr_ports(vendor_list)
    {
        // Invoke the serving logic right away.
        Proceed();
    }

        void Proceed() {
            if (status_ == CREATE) {
                // Make this instance progress to the PROCESS state.
                status_ = PROCESS;

                // As part of the initial CREATE state, we *request* that the system
                // start processing SayHello requests. In this request, "this" acts are
                // the tag uniquely identifying the request (so that different CallData
                // instances can serve different requests concurrently), in this case
                // the memory address of this CallData instance.
                service_->RequestgetProducts(&ctx_, &request_, &responder_, cq_, cq_,
                        this);
            } else if (status_ == PROCESS) {
                // Spawn a new CallData instance to serve new clients while we process
                // the one for this CallData. The instance will deallocate itself as
                // part of its FINISH state.
                new CallData(service_, cq_,addr_ports);
                std::cout <<"Store got request from client for:"<<
                    request_.product_name() << std::endl;
                   store::ProductInfo* info_test = prod_reply.add_products();

                   for(auto i : addr_ports) {
                       std::cout <<"Asking Vendors"<< std::endl;
                       CompletionQueue cq;
                       ClientContext context;
                       BidReply bid_reply;
                       Status cli_status;
                       BidQuery bid;
                       bid.set_product_name(request_.product_name());

                       std::shared_ptr<Channel> channel =
                           grpc::CreateChannel(i,grpc::InsecureChannelCredentials());
                       std::unique_ptr<Vendor::Stub> stub = Vendor::NewStub(channel);
                       std::unique_ptr<ClientAsyncResponseReader<BidReply>>rpc (
                               stub->AsyncgetProductBid(&context,bid, &cq));
                       rpc->Finish(&bid_reply,&cli_status,(void*)1);
                       void* got_tag;
                       bool ok = false;
                       GPR_ASSERT(cq.Next(&got_tag, &ok));
                       GPR_ASSERT(got_tag == (void*)1);
                       GPR_ASSERT(ok);


                       // Taking the request, and submitting it to the vendors
                       if (cli_status.ok()) {
                           info_test->set_price(bid_reply.price());
                           info_test->set_vendor_id(bid_reply.vendor_id());
                       }
                   }


                // The actual processing.

                std::cout <<"In Proceed - PROCESS" << std::endl;

                // And we are done! Let the gRPC runtime know we've finished, using the
                // memory address of this instance as the uniquely identifying tag for
                // the event.
                status_ = FINISH;
                responder_.Finish(prod_reply, Status::OK, this);

            } else {
                std::cout <<"In Proceed - FINISH" << std::endl;
                GPR_ASSERT(status_ == FINISH);
                // Once in the FINISH state, deallocate ourselves (CallData).
                delete this;
            }
        }

    private:
        // The means of communication with the gRPC runtime for an asynchronous
        // server.
        Store::AsyncService* service_;
        // The producer-consumer queue where for asynchronous server notifications.
        ServerCompletionQueue* cq_;
        // Context for the rpc, allowing to tweak aspects of it such as the use
        // of compression, authentication, as well as to send metadata back to the
        // client.
        ServerContext ctx_;

        // What we get from the client.
        ProductQuery request_;
        // What we send back to the client.
        ProductReply prod_reply;

        // The means to get back to the client.
        ServerAsyncResponseWriter<ProductReply> responder_;
        std::vector<std::string> addr_ports;


        // Let's implement a tiny state machine with the following states.
        enum CallStatus { CREATE, PROCESS, FINISH };
        CallStatus status_;  // The current serving state.
};

class StoreImpl final{
    public:
        StoreImpl(std::string port, std::vector<std::string> in_addr_ports):
            port(port), addr_ports(in_addr_ports){}
        ~StoreImpl(){
            store_server->Shutdown();
            comp_q_client->Shutdown();
        }
        void Run() {
            std::string server_address("localhost:"+ port);
            ServerBuilder builder;
            builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
            builder.RegisterService(&store_service);
            comp_q_client = builder.AddCompletionQueue();
            store_server = builder.BuildAndStart();
            std::cout <<"Store listening on " <<server_address <<std::endl;
            HandleRpcs();
        }
        // Class encompasing the state and logic needed to serve a request.

        // This can be run in multiple threads if needed.
        void HandleRpcs() {
            // Spawn a new CallData instance to serve new clients.

            new CallData(&store_service, comp_q_client.get(),addr_ports);

            // uniquely identifies a request.
            // or the tid of the thread in the queue
            void* tag;
            bool ok;
            while (true) {
                // Block waiting to read the next event from the completion queue. The
                // event is uniquely identified by its tag, which in this case is the
                // memory address of a CallData instance.
                // The return value of Next should always be checked. This return value
                // tells us whether there is any kind of event or cq_ is shutting down.
                GPR_ASSERT(comp_q_client->Next(&tag, &ok));
                GPR_ASSERT(ok);
                // run the thread that is on the queue
                static_cast<CallData*>(tag)->Proceed();
            }
        }

        std::unique_ptr<ServerCompletionQueue> comp_q_client;
        Store::AsyncService store_service;
        std::unique_ptr<Server> store_server;
        std::string port;
        std::vector<std::string> addr_ports;


};
void usage(){
    std::cerr << "Please provide path to file containing IP:port pairs\n\
        and a port for the store to listen\n\
        eg. ./store /path/to/address.txt 50050"<<std::endl;
}

int main(int argc, char** argv) {
    if(argc != 3){
        usage();
        return EXIT_FAILURE;
    }
    std::ifstream filename(argv[1]);
    std::string port = argv[2];
    std::vector<std::string> addrs;
    std::string line;
    while(std::getline(filename,line)){
        addrs.push_back(line);
    }
    StoreImpl store(port,addrs);
    store.Run();

    return EXIT_SUCCESS;
}
