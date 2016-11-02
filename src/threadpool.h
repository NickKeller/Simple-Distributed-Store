#pragma once

#include <iostream>
#include <deque>
#include <functional>
#include <thread>
#include <condition_variable>
#include <mutex>
#include <random>

class threadpool {
public:
	threadpool(int num_threads = 4);

	template<class F> void enqueue(F&& f);
	void waitFinished();
private:
	std::vector<std::thread> workers;
	std::deque<std::function<void()>> tasks;

};
