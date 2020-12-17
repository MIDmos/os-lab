#define _GNU_SOURCE

#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <fcntl.h>
#include <string.h>
#include <stdint.h>
#include <limits.h>
#include <sys/file.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <linux/futex.h>
#include <sys/syscall.h>
#include <stdatomic.h>
#include <sys/wait.h>

#define errExit(msg)    do { perror(msg); exit(EXIT_FAILURE);} while (0)

#define MEMORY_SIZE_A 119
#define MEMORY_ADDRESS_B 0x122C7834
#define GENERATOR_THREADS_NUM_D 70
#define FILE_SIZE_E 132
#define BLOCK_SIZE_G 16
#define BLOCK_SIZE_DEFAULT 4096

#define READER_THREADS_NUM_I 128

#define FILES_NUMBER 1

uint32_t file_futex;

int futex(uint32_t *uaddr, int futex_op, uint32_t val) {
    return syscall(SYS_futex, uaddr, futex_op, val, NULL, NULL, 0);
}

void fwait(uint32_t *futexp) {
    long s;

    while (1) {
        const uint32_t one = 1;
        if (atomic_compare_exchange_strong(futexp, &one, 0))
            break;
        s = futex(futexp, FUTEX_WAIT, 0);
        if (s == -1 && errno != EAGAIN)
            errExit("futex-FUTEX_WAIT");
    }
}

void fpost(uint32_t *futexp) {
    long s;

    const uint32_t zero = 0;
    if (atomic_compare_exchange_strong(futexp, &zero, 1)) {
        s = futex(futexp, FUTEX_WAKE, 1);
        if (s == -1)
            errExit("futex-FUTEX_WAKE");
    }
}

typedef struct {
    int id;
} my_thread_data_t;
typedef struct my_thread_t {
    my_thread_data_t data;
    pthread_t thread;
} my_thread_t;


const int SIZE_PER_WRITER_THREAD = MEMORY_SIZE_A * 1024 * 1024 / GENERATOR_THREADS_NUM_D;
const int SIZE_LAST_WRITER_THREAD = MEMORY_SIZE_A * 1024 * 1024 % GENERATOR_THREADS_NUM_D;
const int SIZE_OF_LAST_FILE = MEMORY_SIZE_A % FILE_SIZE_E;

FILE *devurandom_file;
int infinity_loop = 1;
const char *devurandom_filename = "/dev/urandom";
my_thread_t *generators;
my_thread_t *readers;

const char *filenames[] = {"f0"};
unsigned char *memory_actual_address;
int min = INT_MAX;

void init_generator_threads();

void init_reader_threads();

my_thread_t *init_threads(int threads_num);

void *write_to_memory_and_to_files_loop(void *);

void *write_to_memory(void *);

void *write_to_memory_and_to_files(void *thread_data);

void *read_files_loop(void *thread_data);

void start_threads(my_thread_t *writer_arr, int thread_num, void *(*f)(void *));

void stop_threads(my_thread_t *writer_arr, int thread_num);

void *write_to_file(void *);

void seq_write_to_file(int fd, int block_num, int offset);

void *read_file(void *thread_data);

char *seq_read_from_file(int fd, int file_size);

int calculate_min(int *numbers, int buffer_size);

void set_new_min(int new_min);

void *read_files_loop(void *thread_data) {
    while (infinity_loop) {
        read_file(thread_data);
    }
    return NULL;

}

void *read_file(void *thread_data) {
    const my_thread_data_t *data = (my_thread_data_t *) thread_data;
    const int file_id = data->id % FILES_NUMBER;
    const int bytes_to_file = (file_id == FILES_NUMBER - 1 ? SIZE_OF_LAST_FILE : FILE_SIZE_E) * 1024 * 1024;
    int file;

    file = open(filenames[file_id], O_RDONLY, S_IRGRP | S_IWGRP | S_IRUSR | S_IWUSR | S_IROTH | S_IWOTH);
    if (file == -1) {
        printf("Error no is : %d\n", errno);
        printf("[%d]Can't open %s for writing\n", data->id, filenames[file_id]);
        return NULL;
    }

    fwait(&file_futex);
    int *int_buf = (int *) seq_read_from_file(file, bytes_to_file);
    fpost(&file_futex);

    close(file);

    set_new_min(calculate_min(int_buf, bytes_to_file / sizeof(int)));
    free(int_buf);
    return NULL;
}


char *seq_read_from_file(int fd, int file_size) {
    char *buffer = (char *) malloc(file_size);
    int offset;
    for (offset = 0; offset < file_size; offset += BLOCK_SIZE_G) {
        pread(fd, buffer + offset, BLOCK_SIZE_G, offset);
    }
    return buffer;
}


void *write_to_file(void *thread_data) {
    my_thread_data_t *data = (my_thread_data_t *) thread_data;
    const int file_id = data->id % FILES_NUMBER;
    const int bytes_to_file = (file_id == FILES_NUMBER - 1 ? SIZE_OF_LAST_FILE : FILE_SIZE_E) * 1024 * 1024;
//    const int block_num = bytes_to_file / BLOCK_SIZE_DEFAULT;
    int file;
    struct stat st;
    int size;

    int flag = O_WRONLY | O_CREAT | O_APPEND | O_DIRECT;
    //O_WRONLY|O_CREAT|O_SYNC, S_IRGRP|S_IWGRP|S_IRUSR|S_IWUSR|S_IROTH|S_IWOTH
    file = open(filenames[file_id], flag);
    //printf("[%d] write to f%d (%d). \n", data->id, file_id, file);

    if (file == -1) {
        printf("Error no is : %d\n", errno);
        printf("[%d]Can't open %s for writing\n", data->id, filenames[file_id]);
        return NULL;
    }

    while (1) {
        fwait(&file_futex);
        file = open(filenames[file_id], flag);
        if (stat(filenames[file_id], &st) == 0) {
            size = st.st_size;
            if (size >= bytes_to_file) {
                close(file);
                //printf("[%d] End write to f%d (%d). \n", data->id, file_id, file);
                fpost(&file_futex);
                return NULL;
            } else {
                //printf("[%d] write to f%d (%d). \n", data->id, file_id, size);
                seq_write_to_file(file, 1, size * 1024 * 1024);
                close(file);
                fpost(&file_futex);
            }
        } else {
            errExit("Can't get file size");
        }
    }
}

void seq_write_to_file(int fd, int block_num, int offset) {
    const int align = BLOCK_SIZE_DEFAULT - 1;
    unsigned char *buff = malloc((int) BLOCK_SIZE_DEFAULT + align);
    unsigned char *wbuff = (unsigned char *) (((uintptr_t) buff + align) & ~((uintptr_t) align));
    int i;
    for (i = 0; i < block_num; i++) {
        memcpy(wbuff, memory_actual_address + offset, BLOCK_SIZE_DEFAULT);
        if (pwrite(fd, wbuff, BLOCK_SIZE_DEFAULT, BLOCK_SIZE_DEFAULT * i) < 0) {
            printf("Can't write to file(%d). Loop - %d/%d. Errno -  %d\n", fd, i + 1, block_num, errno);
            break;
        }
        offset += BLOCK_SIZE_DEFAULT;
    }
    free(buff);
}

void start_threads(my_thread_t *writer_arr, int thread_num, void *(*f)(void *)) {
    int i;
    for (i = 0; i < thread_num; i++) {
        pthread_create(&(writer_arr[i].thread), NULL, f, &(writer_arr[i].data));
    }
}

void stop_threads(my_thread_t *writer_arr, int thread_num) {
    int i;
    for (i = 0; i < thread_num; i++) {
        pthread_join(writer_arr[i].thread, NULL);
    }

}

void *write_to_memory(void *thread_data) {
    my_thread_data_t *data = (my_thread_data_t *) thread_data;

    fread(memory_actual_address + data->id * SIZE_PER_WRITER_THREAD, 1,
          data->id == GENERATOR_THREADS_NUM_D - 1 ? SIZE_LAST_WRITER_THREAD : SIZE_PER_WRITER_THREAD, devurandom_file);

    return NULL;
}

void *write_to_memory_and_to_files(void *thread_data) {
    write_to_memory(thread_data);
    write_to_file(thread_data);
    return NULL;
}

void *write_to_memory_and_to_files_loop(void *thread_data) {
    while (infinity_loop) {
        void *write_to_memory_and_to_files(void *thread_data);
    }

    return NULL;
}

void init_generator_threads() {
    generators = init_threads(GENERATOR_THREADS_NUM_D);
}

void init_reader_threads() {
    readers = init_threads(READER_THREADS_NUM_I);
}

my_thread_t *init_threads(int threads_num) {
    my_thread_t *threads = malloc(threads_num * sizeof(my_thread_t));
    int i;
    for (i = 0; i < threads_num; i++) {
        threads[i].data.id = i;
    }
    return threads;
}

int calculate_min(int *numbers, int buffer_size) {
    int local_min = INT_MAX;
    for (int i = 0; i < buffer_size; i++) {
        local_min = numbers[i] > local_min ? local_min : numbers[i];
    }
    return local_min;
}

void set_new_min(int new_min) {
    if (new_min < min) {
        printf("New min value! (%d -> %d)\n", min, new_min);
        min = new_min;
    }
}


int main(int argc, char *argv[]) {
    int flag = O_WRONLY | O_CREAT | O_TRUNC | O_DIRECT;
    int do_start_loop;
    printf("pid - %ld\n", (long) getpid());

    file_futex = 1;
    //&mmap(NULL, sizeof(*file_futex), PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_SHARED, -1, 0)[0];

    printf("Before the allocation %d Kib of memory at the address %p. Pause", MEMORY_SIZE_A * 1024,
           (void *) MEMORY_ADDRESS_B);
    getchar();
    memory_actual_address = malloc(MEMORY_SIZE_A * 1024 * 1024);
    printf("Data is allocated at the address %p. Pause", memory_actual_address);
    getchar();


    close(open("f0", flag));

    devurandom_file = fopen(devurandom_filename, "r");
    init_generator_threads();
    init_reader_threads();

    //fill memory at first time;
    puts("Write data to the memory. Wait");
    start_threads(generators, GENERATOR_THREADS_NUM_D, write_to_memory_and_to_files);
    stop_threads(generators, GENERATOR_THREADS_NUM_D);
    puts("The memory is filled. Reading...");
    start_threads(readers, READER_THREADS_NUM_I, read_file);
    stop_threads(readers, READER_THREADS_NUM_I);
    printf("Press enter to start deallocation");
    getchar();

    fclose(devurandom_file);
    free(generators);
    free(readers);
    munmap(memory_actual_address, MEMORY_SIZE_A * 1024 * 1024);
    printf("After deallocation. Pause");
    getchar();


    printf("Start a loop? (1 or not)\n");
    if (scanf("%d", &do_start_loop) == 1 && do_start_loop == 1) {
        //start loop
        printf("The loop is started. Press enter to stop the loop");
        devurandom_file = fopen(devurandom_filename, "r");
        init_generator_threads();
        init_reader_threads();
        memory_actual_address = mmap((void *) MEMORY_ADDRESS_B, MEMORY_SIZE_A * 1024 * 1024, PROT_READ | PROT_WRITE,
                                     MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        start_threads(generators, GENERATOR_THREADS_NUM_D, write_to_memory_and_to_files_loop);
        start_threads(readers, READER_THREADS_NUM_I, read_files_loop);

        getchar();
        infinity_loop = 0;

        stop_threads(generators, GENERATOR_THREADS_NUM_D);
        stop_threads(readers, READER_THREADS_NUM_I);

        fclose(devurandom_file);
        free(generators);
        free(readers);
        munmap(memory_actual_address, MEMORY_SIZE_A * 1024 * 1024);
        getchar();
    }
    return 0;
}
