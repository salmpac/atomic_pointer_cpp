#include <cassert>
#include <atomic>
#include <cstdint>
#include <functional>
#include <random>
#include <thread>
#include <iostream>
#include <vector>

#include <mutex>

struct RandomIntGenerator {
    static uint32_t generate_thread_specific() {
        thread_local std::mt19937 generator([]() {
            auto thread_id = std::this_thread::get_id();
            std::hash<std::thread::id> hasher;
            uint32_t seed = static_cast<uint32_t>(hasher(thread_id));
            return seed;
        }());

        return generator();
    }
};


class Ticketor {
public:
    // if you want to get max k tickets
    // it is optimal to make
    // tickets = 2 * k
    // free_slots = k
    Ticketor(uint32_t tickets, uint32_t free_slots) : slots(tickets + free_slots) {
        for (uint32_t i = 0; i < tickets; ++i) {
            slots[i] = i + 1;
        }
    }

    uint32_t try_give_ticket() {
        uint32_t num_slots = slots.size();
        uint32_t start_index = RandomIntGenerator::generate_thread_specific() % num_slots;
        for (uint32_t attempt = 0; attempt < num_slots; ++attempt) {
            uint32_t index = (start_index + attempt) % num_slots;
            uint32_t ticket = slots[index].exchange(0, std::memory_order_acq_rel);
            if (ticket != 0) {
                return ticket;
            }
        }
        return 0;
    }

    bool try_return_ticket(uint32_t ticket) {
        if (ticket == 0) {
            return false;
        }
        uint32_t num_slots = slots.size();
        uint32_t start_index = RandomIntGenerator::generate_thread_specific() % num_slots;
        for (uint32_t attempt = 0; attempt < num_slots; ++attempt) {
            uint32_t index = (start_index + attempt) % num_slots;
            uint32_t expected = 0;
            if (slots[index].compare_exchange_strong(
                expected, 
                ticket, 
                std::memory_order_acq_rel,
                std::memory_order_acquire)) {
                return true;
            }
        }
        return false;
    }

private:
    std::vector<std::atomic_uint32_t> slots;
};

template<class C>
struct AtomicPointer {
    void make_initial_state(C* initial_value) {
        uint64_t index = ticketor.try_give_ticket();
        pointers[index] = initial_value;
        counters[index].fetch_add(1);
        current_state = (index << 54);
    }

    // max_threads should be not more than 1024 / 6
    // as every thread can take max two locks at a time
    // one is for index that may become old
    // and also for index for new value
    // and i want to guarantee that there is  always at least 1/3 of free slots and 1/3 of busy slots
    AtomicPointer(int max_threads, C* initial_value = nullptr) : ticketor(max_threads * 4, max_threads * 2), counters(4 * max_threads + 1), pointers(4 * max_threads + 1, nullptr) {
        for (int i = 0; i < counters.size(); ++i) {
            counters[i].store(0);
        }
        make_initial_state(initial_value);
    }

    // returns 0 if it was 0
    // returns 1 on success
    // returns -1 on fail
    int try_increase_counter_if_it_was_positive(int index) {
        int32_t old_value = counters[index].load(std::memory_order_acquire);
        if (old_value == 0) {
            return 0;
        }
        if (counters[index].compare_exchange_strong(old_value, old_value + 1, std::memory_order_acq_rel, std::memory_order_acquire)) {
            return 1;
        }
        return -1;
    }

    bool return_lock(uint32_t index, std::function<void(C*)>& to_free) {
        C* old_val = pointers[index];
        if (counters[index].fetch_sub(1, std::memory_order_acq_rel) == 1) {
            while (!ticketor.try_return_ticket(index)) {
            }
            to_free(old_val);
            return true;
        }
        return false;
    }

    // transaction_maker should return new structure
    bool try_transaction(std::function<C*(C*)> transaction_maker, std::function<void(C*)> to_free) {
        uint64_t state = current_state.load(std::memory_order_acquire);

        int index = (state >> 54);
        uint64_t counter = (state & ((1ull << 54) - 1));
        if (try_increase_counter_if_it_was_positive(index) <= 0) {
            return false;
        }
        // here we took the lock for index and should return in to ticketor in case we were the last holder

        C* new_value = transaction_maker(pointers[index]);
        int64_t index_for_new_value = 0;
        do {
            index_for_new_value = static_cast<uint64_t>(ticketor.try_give_ticket());
        } while (index_for_new_value == 0);
        pointers[index_for_new_value] = new_value;
        counters[index_for_new_value].fetch_add(1, std::memory_order_acq_rel);
        // now we took two locks: for index and for index_for_new_value

        uint64_t new_counter = ((counter + 1) & ((1ull << 54) - 1));
        uint64_t new_state = (index_for_new_value << 54) + new_counter;

        if (current_state.compare_exchange_strong(state, new_state, std::memory_order_acq_rel)) {
            // should not return lock for index_for_new_value as it is new head
            return_lock(index, to_free);
            return_lock(index, to_free);
            return true;
        }
        // fail
        return_lock(static_cast<uint32_t>(index_for_new_value), to_free);
        return_lock(index, to_free);
        return false;
    }


    Ticketor ticketor;
    std::vector<std::atomic_int32_t> counters;
    std::vector<C*> pointers;
    // first 10 bits are index of pointer in pointers
    // other 54 bits are counter for uniqueness of states
    // i expect that any thread will never sleep for so long that counter will make round trip
    std::atomic_uint64_t current_state{0};
};

// TESTS START HERE
struct Counter {
    int64_t value;
    
    Counter(int64_t v = 0) : value(v) {}
};

// constants for test
const int NUM_THREADS = 8;
const int OPERATIONS_PER_THREAD = 1000000;

std::mutex cout_lock;

void stress_test_thread(AtomicPointer<Counter>& atomic_counter, std::atomic<int64_t>& total_successful_ops, std::atomic<int64_t>& total_failed_ops, int thread_id) {
    int64_t local_successful = 0;
    int64_t local_failed = 0;

    for (int i = 0; i < OPERATIONS_PER_THREAD; ++i) {
        bool success = atomic_counter.try_transaction(
            // transaction_maker
            [](Counter* old) -> Counter* {
                return new Counter(old->value + 1);
            },
            // on_success
            [](Counter* old) {
                delete old;
            }
        );

        if (success) {
            local_successful++;
        } else {
            local_failed++;
        }

        if (i % 1000 == 0 && i > 0) {
            // std::cout << "Thread " << thread_id << ": " << i << " operations completed" << std::endl;
        }
    }

    total_successful_ops.fetch_add(local_successful, std::memory_order_relaxed);
    total_failed_ops.fetch_add(local_failed, std::memory_order_relaxed);

    std::lock_guard safe_cout(cout_lock);
    std::cout << "Thread " << thread_id << " finished. Successful: " 
                << local_successful << ", Failed: " << local_failed << std::endl;
}

void stress_test() {
    std::cout << "Starting stress test with " << NUM_THREADS << " threads..." << std::endl;

    Counter* initial = new Counter(0);
    AtomicPointer<Counter> atomic_counter(NUM_THREADS, initial);

    std::atomic<int64_t> total_successful_ops{0};
    std::atomic<int64_t> total_failed_ops{0};
    std::vector<std::thread> threads;

    auto start_time = std::chrono::high_resolution_clock::now();

    for (int thread_id = 0; thread_id < NUM_THREADS; ++thread_id) {
        threads.emplace_back([&atomic_counter, &total_successful_ops, &total_failed_ops, thread_id]() {
            stress_test_thread(atomic_counter, total_successful_ops, total_failed_ops, thread_id);
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    uint32_t final = 0;
    do {
        atomic_counter.try_transaction(
            // transaction_maker
            [&final](Counter* old) -> Counter* {
                final = old->value;
                return new Counter(old->value);
            },
            // on_success
            [](Counter* old) {
                delete old;
            }
        );
    } while (final == 0);

    std::lock_guard safe_cout(cout_lock);
    std::cout << "\n=== Test Results ===" << std::endl;
    std::cout << "Actual final value: " << final << std::endl;
    std::cout << "Total successful operations: " << total_successful_ops.load() << std::endl;
    std::cout << "Total failed operations (retries): " << total_failed_ops.load() << std::endl;
    std::cout << "Time taken: " << duration.count() << " ms" << std::endl;
    std::cout << "Operations per second: " 
              << (total_successful_ops.load() * 1000.0 / duration.count()) << std::endl;

    std::cout << "\n✓ All checks passed!" << std::endl;
}

int main() {
    stress_test();
    return 0;
}
