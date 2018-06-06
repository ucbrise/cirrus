#ifndef _SYNCHRONIZATION_H_
#define _SYNCHRONIZATION_H_

#include <errno.h>
#include <semaphore.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <atomic>
#include <iostream>
#include <string>
#include <stdexcept>
#include <algorithm>
#include <ctime>
#include <random>
#include <thread>
#include "Decls.h"

namespace cirrus {

/**
  * A class providing a general outline of a lock. Purely virtual.
  */
class Lock {
 public:
    Lock() = default;
    virtual ~Lock() = default;

    virtual void wait() = 0; /** A pure virtual member. */
    virtual void signal() = 0; /** A pure virtual member. */
    virtual void signal(int count) = 0; /** A pure virtual member. */

    /**
      * A pure virtual member.
      * @return true if lock has succeeded
      */
    virtual bool trywait() = 0;

 private:
    DISALLOW_COPY_AND_ASSIGN(Lock);
};

/**
  * A class that extends the Lock class. Makes use of sem_t and its
  * associated methods to fullfill the functions of the lock class.
  */
class PosixSemaphore : public Lock {
 public:
    explicit PosixSemaphore(int initialCount = 0) : Lock() {
        #ifdef __APPLE__
        sem_name = random_string();
        m_sema = sem_open(sem_name.c_str(), O_CREAT, S_IRWXU, initialCount);
        if (m_sema == SEM_FAILED) {
            std::cout << "errno is: " << errno << std::endl;
            std::cout << "Name is: " << sem_name << std::endl;
            throw std::runtime_error("Creation of new semaphore failed");
        }
        #else
        sem_init(&m_sema, 0, initialCount);
        #endif  // __APPLE__
    }

    virtual ~PosixSemaphore() {
#ifdef __APPLE__
        sem_close(m_sema);
        sem_unlink(sem_name.c_str());
#else
        sem_destroy(&m_sema);
#endif  // __APPLE__
    }

    /**
      * Waits until entered into semaphore.
      */
    void wait() final {
#ifdef __APPLE__
        int rc = sem_wait(m_sema);
        while (rc == -1 && errno == EINTR) {
            rc = sem_wait(m_sema);
        }
#else
        int rc = sem_wait(&m_sema);
        while (rc == -1 && errno == EINTR) {
            rc = sem_wait(&m_sema);
        }
#endif  // __APPLE__
    }

    /**
      * Posts to one waiter
      */
    void signal() final {
#ifdef __APPLE__
        sem_post(m_sema);
#else
        sem_post(&m_sema);
#endif  // __APPLE__
    }

    /**
      * Posts to a specified number of waiters
      * @param count number of waiters to wake
      */
    void signal(int count) final {
        while (count-- > 0) {
#ifdef __APPLE__
            sem_post(m_sema);
#else
            sem_post(&m_sema);
#endif  // __APPLE__
        }
    }

    /**
      * Attempts to lock the semaphore and returns its success.
      * @return True if the semaphore had a positive value and was decremented.
      */
    bool trywait() final {
#ifdef __APPLE__
        int ret = sem_trywait(m_sema);
#else
        int ret = sem_trywait(&m_sema);
#endif  // __APPLE__
        if (ret == -1 && errno != EAGAIN) {
            throw std::runtime_error("trywait error");
        }
        return ret != -1;  // true for success
    }

    int getvalue() {
      int value;
#ifdef __APPLE__
      throw std::runtime_error("Not supported");
#else
      int ret = sem_getvalue(&m_sema, &value);
#endif  // __APPLE__
      if (ret == -1) {
        throw std::runtime_error("sem_getvalue error");
      }
      return value;
    }

 private:
#ifdef __APPLE__
    /** Underlying semaphore that operations are performed on. */
    sem_t *m_sema;
    /** Name of the underlying semaphore. */
    std::string sem_name;
    /** Length of randomly names for semaphores. */
    const int rand_string_length = 16;

    uint64_t x = 123456789, y = 362436069, z = 521288629;
    uint64_t xorshf96(void) {
        uint64_t temp;
        x ^= x << 16;
        x ^= x >> 5;
        x ^= x << 1;

        temp = x;
        x = y;
        y = z;
        z = temp ^ x ^ y;
        return z;
    }

    /**
     * Method to generate random strings for named semaphores.
     */
    std::string random_string() {
        const char charset[] =
            "0123456789"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "abcdefghijklmnopqrstuvwxyz";

        const size_t max_index = (sizeof(charset) - 1);
        // Seed RNG
        const auto time_seed = static_cast<size_t>(std::time(0));
        const auto clock_seed = static_cast<size_t>(std::clock());
        const size_t pid_seed =
            std::hash<std::thread::id>()(std::this_thread::get_id());

        std::seed_seq seed_value { time_seed, clock_seed, pid_seed };
        seed_value.generate(&x, &x + 1);

        std::string ret_string;
        // First character of name must be a slash
        ret_string.push_back('/');

        for (int i = 1; i < rand_string_length; i++) {
            char next_char = charset[xorshf96() % max_index];
            ret_string.push_back(next_char);
        }
        return ret_string;
    }
#else
    sem_t m_sema; /**< underlying semaphore that operations are performed on. */
#endif  // __APPLE__
};

/**
  * A lock that extends the Lock class. Utilizes spin waiting.
  */
class SpinLock : public Lock {
 public:
    SpinLock() :
        Lock()
    { }

    virtual ~SpinLock() = default;

    /**
      * This function busywaits until it obtains the lock.
      */
    void wait() final {
        while (lock.test_and_set(std::memory_order_acquire))
            continue;
    }

    /**
      * This function attempts to obtain the lock once.
      * @return true if the lock was obtained, false otherwise.
      */
    bool trywait() final {
        return lock.test_and_set(std::memory_order_acquire) == 0;
    }

    void signal(__attribute__((unused)) int count) final {
        throw std::runtime_error("Not implemented");
    }

    void signal() final {
        lock.clear(std::memory_order_release);
    }

 private:
    std::atomic_flag lock = ATOMIC_FLAG_INIT;
};

}  // namespace cirrus

#endif  // _SYNCHRONIZATION_H_
