#include <mpi.h>
#include <unistd.h>
#include <stdlib.h>
#include <cstdint>
#include <iostream>
#include <map>
#include <string>
#include <cctype>
#include <chrono>
#include <memory>

#include "object_store/FullBladeObjectStore.h"
#include "tests/object_store/object_store_internal.h"
#include "utils/CirrusTime.h"
#include "utils/Log.h"
#include "utils/Stats.h"
#include "client/TCPClient.h"
#include "common/Exception.h"
#include "Utils.h"

#define OBJ_ID 1000

// This is used for non-array objects
template<typename T>
class serializer : public cirrus::Serializer<T> {
 public:
    explicit serializer(const std::string& name = "") :
        name(name) {}

    uint64_t size(const T& /*obj*/) const override {
        return sizeof(T);
    }

    void serialize(const T& obj, void* mem) const override {
        // copy samples to array
        memcpy(mem, &obj, size(obj));
    }
 private:
    std::string name;  //< name associated with this serializer
};

/* Takes a pointer to raw mem passed in and returns as object. */
template<typename T, unsigned int SIZE>
T deserializer(const void* data, unsigned int /* size */) {
    const T *ptr = reinterpret_cast<const T*>(data);
    T ret;
    std::memcpy(&ret, ptr, SIZE);
    return ret;
}

void sleep_forever() {
    while (1) {
        sleep(1000);
    }
}

const char PORT[] = "12345";
const char IP[] = "10.10.49.87";

void run_task_2() {
    std::cout << "Worker task connecting to store" << std::endl;

    cirrus::TCPClient client;
    // this is used to access the training labels
    serializer<int> ser;
    cirrus::ostore::FullBladeObjectStoreTempl<int>
        int_store(IP, PORT, &client,
                ser, deserializer<int, sizeof(int)>);

    int d = int_store.get(OBJ_ID);
    if (d == 42) {
        std::cout << "Task 2 got correct result" << std::endl;
    } else {
        std::cout << "Task2 got incorrect result" << std::endl;
    }
}

void run_task_1() {
    std::cout << "Worker task connecting to store" << std::endl;

    cirrus::TCPClient client;
    // this is used to access the training labels
    serializer<int> ser;
    cirrus::ostore::FullBladeObjectStoreTempl<int>
        int_store(IP, PORT, &client,
                ser, deserializer<int, sizeof(int)>);

    int n = 42;
    int_store.put(OBJ_ID, n);

    sleep(1);
    int d = int_store.get(OBJ_ID);
    if (d == 42) {
        std::cout << "Task1 correct result" << std::endl;
    } else {
        std::cout << "Task1 incorrect result" << std::endl;
    }
}

/**
  * Load the object store with the training dataset
  * It reads from the criteo dataset files and writes to the object store
  * It signals when work is done by changing a bit in the object store
  */

void run_tasks(int rank) {
    std::cout << "Run tasks rank: " << rank << std::endl;
    if (rank == 0) {
        run_task_1();
        sleep_forever();
    } else if (rank == 1) {
        sleep(6);
        run_task_2();
        sleep_forever();
    } else {
        throw std::runtime_error("Wrong number of tasks");
    }
}

inline
void init_mpi(int argc, char**argv) {
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    if (provided != MPI_THREAD_MULTIPLE) {
        std::cerr
            << "MPI implementation does not support multiple threads"
            << std::endl;
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
}

/**
  * Cirrus provides an interface for registering tasks
  * A task has a name, an entry point (function) and required resources
  * Once tasks are registered, a description of the tasks to be run can be generated
  * This description can be used to reserve a set of amazon instances where the tasks can be deployed
  * ./system --generate
  * ./system --run --task=1
  */ 
int main(int argc, char** argv) {
    std::cout << "Starting parameter server" << std::endl;

    int rank, nprocs;

    init_mpi(argc, argv);
    int err = MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    check_mpi_error(err);
    err = MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    check_mpi_error(err);

    char name[200];
    gethostname(name, 200);
    std::cout << "MPI multi task test running on hostname: " << name
        << " with rank: " << rank
        << std::endl;

    run_tasks(rank);

    MPI_Finalize();
    std::cout << "Test successful" << std::endl;

    return 0;
}

