#pragma once

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>

namespace duckdb {

template <typename Job, typename TimeoutHandler>
void WaitForNatsJobWorker(std::unique_lock<std::mutex> &job_lock, Job &job, TimeoutHandler timeout_handler) {
	if (!job.worker_finished &&
	    !job.cv.wait_for(job_lock, std::chrono::seconds(30), [&]() { return job.worker_finished; })) {
		timeout_handler();
	}
}

template <typename Job>
void JoinNatsJobWorker(Job &job) {
	if (job.worker.joinable()) {
		job.worker.join();
	}
}

} // namespace duckdb
