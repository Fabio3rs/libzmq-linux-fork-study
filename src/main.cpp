#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <error.h>
#include <exception>
#include <fcntl.h>
#include <fmt/core.h>
#include <fstream>
#include <netdb.h>
#include <netinet/in.h>
#include <sstream>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/un.h>
#include <thread>
#include <tuple>
#include <unistd.h>
#include <utility>
#include <zmq.hpp>
#include <zmq_addon.hpp>

namespace boost {
namespace serialization {

template <typename Archive, typename... Types>
void serialize(Archive &ar, std::tuple<Types...> &t, const unsigned int) {
    std::apply([&](auto &...element) { ((ar & element), ...); }, t);
}

} // namespace serialization
} // namespace boost

namespace {

int create_unix_sock(std::string_view path) {
    int sockfd, connfd;
    struct sockaddr_un servaddr, cliaddr;

    // Create socket
    sockfd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sockfd == -1) {
        perror("Socket creation failed");
        exit(EXIT_FAILURE);
    }

    int rres = remove(path.data());
    fmt::println("rres: {}", rres);

    // Initialize server address
    memset(&servaddr, 0, sizeof(servaddr));
    servaddr.sun_family = AF_UNIX;
    strncpy(servaddr.sun_path, path.data(), sizeof(servaddr.sun_path) - 1);

    // Bind the socket
    if (bind(sockfd, (struct sockaddr *)&servaddr, sizeof(servaddr)) == -1) {
        perror("Socket bind failed");
        exit(EXIT_FAILURE);
    }

    // Listen for connections
    if (listen(sockfd, 5) == -1) {
        perror("Listen failed");
        exit(EXIT_FAILURE);
    }

    return sockfd;
}

int connect_unix(std::string_view path) {
    struct sockaddr_un addr = {.sun_family = AF_UNIX, .sun_path = ""};

    if (path.size() > sizeof(addr.sun_path) - 1)
        return -1;

    strncpy(addr.sun_path, path.data(), sizeof(addr.sun_path) - 1);

    int unix_sock = socket(AF_UNIX, SOCK_STREAM, 0);
    if (unix_sock == -1)
        return -1;

    if (connect(unix_sock, (struct sockaddr *)&addr, sizeof(addr)) == -1) {
        close(unix_sock);
        return -1;
    }

    return unix_sock;
}

int send_fd(int unix_sock, int fd) {
    char iov_basemsg[] = "Hello, World!";
    struct iovec iov = {.iov_base = reinterpret_cast<void *>(
                            iov_basemsg), // Must send at least one byte
                        .iov_len = 2};

    union {
        char buf[CMSG_SPACE(sizeof(fd))];
        struct cmsghdr align;
    } u;

    struct msghdr msg = {.msg_iov = &iov,
                         .msg_iovlen = 1,
                         .msg_control = u.buf,
                         .msg_controllen = sizeof(u.buf)};

    struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);
    *cmsg = (struct cmsghdr){
        .cmsg_len = CMSG_LEN(sizeof(fd)),
        .cmsg_level = SOL_SOCKET,
        .cmsg_type = SCM_RIGHTS,
    };

    memcpy(CMSG_DATA(cmsg), &fd, sizeof(fd));

    return sendmsg(unix_sock, &msg, 0);
}

std::string last_endpoint;
int parentProcessId;
int serverfd = 0;

bool running = true;

template <class args_tuple_t> struct CallData {
    args_tuple_t argument_tuple;
    uint64_t thread_start_fn;

    template <class Archive>
    void serialize(Archive &ar, const unsigned int version) {
        ar & argument_tuple;
        ar & thread_start_fn;
    }
};

struct FdShare {
    int fd{};
    std::string path;

    void receive() {
        fd = -1;
        int connfd = connect_unix(path);

        if (connfd == -1) {
            fmt::println("connect_unix failed {}", path);
            return;
        }

        // Create the message structure
        struct msghdr msg = {0};
        struct iovec iov[1];
        char buf[1];
        iov[0].iov_base = buf;
        iov[0].iov_len = 1;
        msg.msg_iov = iov;
        msg.msg_iovlen = 1;

        // Receive the message with the file descriptor
        char control[CMSG_SPACE(sizeof(int))];
        msg.msg_control = control;
        msg.msg_controllen = sizeof(control);
        if (recvmsg(connfd, &msg, 0) <= 0) {
            perror("Recvmsg failed");
            exit(EXIT_FAILURE);
        }

        // Extract the file descriptor
        struct cmsghdr *cmptr = CMSG_FIRSTHDR(&msg);
        if (cmptr && cmptr->cmsg_len == CMSG_LEN(sizeof(int))) {
            if (cmptr->cmsg_level != SOL_SOCKET ||
                cmptr->cmsg_type != SCM_RIGHTS) {
                perror("Control level or type not correct");
                exit(EXIT_FAILURE);
            }
            fd = *((int *)CMSG_DATA(cmptr));
        } else {
            perror("Invalid message");
            exit(EXIT_FAILURE);
        }

        fmt::println("fd RECEIVED: {}", fd);
    }

    static void sendToCli(int fd, std::string path) {
        remove(path.c_str());
        int unix_sock = create_unix_sock(path);

        if (unix_sock == -1) {
            fmt::println("CREATE failed");
            return;
        }

        struct sockaddr_un cliaddr {};
        socklen_t addrlen = sizeof(cliaddr);

        fmt::println("WAITING CONNECTION fd: {}", fd);

        int connfd = accept(unix_sock, (struct sockaddr *)&cliaddr, &addrlen);
        if (connfd < 0) {
            perror("Server accept failed");
            exit(EXIT_FAILURE);
        }

        send_fd(connfd, fd);

        close(connfd);
        close(unix_sock);

        std::this_thread::yield();

        close(fd);
    }

    template <class Archive>
    void serialize(Archive &ar, const unsigned int version) {
        bool toload = path.empty();

        ar & fd;
        ar & path;

        fmt::println("toload: {}; fd {}; path {}", toload, fd, path);

        if (toload) {
            receive();
        } else {
            std::thread(sendToCli, fd, path).detach();
        }
    }

    FdShare() = default;
    FdShare(int fd, std::string path) : fd(fd), path(std::move(path)) {}
    explicit FdShare(int fd) : fd(fd) {
        path = "/tmp/sockfd_path_" + std::to_string(getpid()) + "_" +
               std::to_string(fd);
    }

    FdShare(const FdShare &other) = default;
    FdShare(FdShare &&other) = default;
    FdShare &operator=(const FdShare &other) = default;
    FdShare &operator=(FdShare &&other) = default;

    ~FdShare() = default;
};

template <typename rawfn_T, class... Types>
void callFn(rawfn_T fun, Types &&...args) {
    fun(std::forward<Types>(args)...);
}

template <typename rawfn_T, class... Types>
void callWrapFn(const std::string &data) {
    std::stringstream ifs(data);
    boost::archive::text_iarchive ia(ifs);
    CallData<std::tuple<Types...>> call_data;
    ia >> call_data;

    std::apply(reinterpret_cast<rawfn_T>(call_data.thread_start_fn),
               call_data.argument_tuple);
}

template <typename rawfn_T, class... Types>
std::pair<void *, std::string> prepareFn(rawfn_T fun, Types &&...args) {
    using args_tuple_t = decltype(std::tuple{std::forward<Types>(args)...});

    CallData<args_tuple_t> call_data;

    call_data.argument_tuple = std::tuple{std::forward<Types>(args)...};

    call_data.thread_start_fn = reinterpret_cast<uint64_t>(fun);

    std::stringstream ofs;
    boost::archive::text_oarchive oa(ofs);
    oa << call_data;

    void *ptr = reinterpret_cast<void *>(callWrapFn<rawfn_T, Types...>);

    return {ptr, ofs.str()};
}

void signalHandler(int signal) {
    fmt::println("signalHandler: {}", signal);
    running = false;
    close(serverfd);
}

void receiveMessages() {
    zmq::context_t ctx;

    zmq::socket_t sock2(ctx, zmq::socket_type::pull);
    sock2.connect(last_endpoint);
    std::vector<zmq::message_t> recv_msgs;
    while (running) {
        recv_msgs.clear();
        const auto ret =
            zmq::recv_multipart(sock2, std::back_inserter(recv_msgs));
        if (!ret) {
            fmt::println("Recv failed");
            return;
        }

        fmt::println("recv_msgs[0]: {}", *ret);

        for (const auto &msg : recv_msgs) {
            fmt::println(
                "msg: {}",
                std::string_view(reinterpret_cast<const char *>(msg.data()),
                                 msg.size()));
        }

        if (recv_msgs.size() < 2) {
            fmt::println("recv_msgs.size() < 2");
            return;
        }

        if (std::string_view(
                reinterpret_cast<const char *>(recv_msgs[0].data()),
                recv_msgs[0].size()) == "RPC") {
            fmt::println("RPC");
            auto ptr =
                std::string(reinterpret_cast<const char *>(recv_msgs[1].data()),
                            recv_msgs[1].size());

            auto fn = static_cast<uint64_t>(std::stoull(ptr));
            auto data = std::string_view(
                reinterpret_cast<const char *>(recv_msgs[2].data()),
                recv_msgs[2].size());
            auto fnptr =
                reinterpret_cast<void (*)(const std::string &data)>(fn);
            fnptr(std::string(data));
            continue;
        }

        auto fdstr =
            std::string(reinterpret_cast<const char *>(recv_msgs[1].data()),
                        recv_msgs[1].size());

        int connfd = connect_unix(fdstr);

        if (connfd == -1) {
            fmt::println("connect_unix failed");
            return;
        }

        // Create the message structure
        struct msghdr msg = {0};
        struct iovec iov[1];
        char buf[1];
        iov[0].iov_base = buf;
        iov[0].iov_len = 1;
        msg.msg_iov = iov;
        msg.msg_iovlen = 1;

        // Receive the message with the file descriptor
        char control[CMSG_SPACE(sizeof(int))];
        msg.msg_control = control;
        msg.msg_controllen = sizeof(control);
        if (recvmsg(connfd, &msg, 0) <= 0) {
            perror("Recvmsg failed");
            exit(EXIT_FAILURE);
        }

        // Extract the file descriptor
        struct cmsghdr *cmptr = CMSG_FIRSTHDR(&msg);
        int fd = -1;
        if (cmptr && cmptr->cmsg_len == CMSG_LEN(sizeof(int))) {
            if (cmptr->cmsg_level != SOL_SOCKET ||
                cmptr->cmsg_type != SCM_RIGHTS) {
                perror("Control level or type not correct");
                exit(EXIT_FAILURE);
            }
            fd = *((int *)CMSG_DATA(cmptr));
        } else {
            perror("Invalid message");
            exit(EXIT_FAILURE);
        }

        // Use the file descriptor

        fmt::println("fd: {}", fd);

        write(fd, "Hello world!\n", strlen("Hello world!\n"));

        close(fd);
        /*
                auto fdstr =
                    std::string(reinterpret_cast<const char
           *>(recv_msgs[1].data()), recv_msgs[1].size()); auto fd =
           std::stoi(fdstr);

                fmt::println("fd: {}", fd);

                system(("ls -l /proc/" + std::to_string(parentProcessId) +
           "/fd/" + std::to_string(fd)) .c_str());
                // fd = syscall(SYS_pidfd_getfd, parentProcessId, fd, 0);
                auto spath =
                    ("/proc/" + std::to_string(parentProcessId) + "/fd/" +
           fdstr); fmt::println("spath: {}", spath); fd = open(spath.c_str(),
           O_RDWR | O_CLOEXEC);

                fmt::println("fd: {}", fd);

                if (fd < 0) {
                    fmt::println("pidfd_getfd failed: {} errno str {}", fd,
                                 strerror(errno));
                    return;
                }

                fmt::println("pidfd_getfd success: {}", fd);

                write(fd, "Hello world!\n", strlen("Hello world!\n"));
                close(fd); */
    }
}

void algumacoisaFn(std::string_view data, int x, int y, int z) {
    fmt::println("current pid {}, algumacoisaFn: {}; x {}; y {}; z {}",
                 getpid(), data, x, y, z);
}

void rpcSocket(FdShare fd) {
    int wres = write(fd.fd, "Hello world!\n", strlen("Hello world!\n"));
    fmt::println("wres: {}", wres);
    close(fd.fd);
}

void sendRPC(zmq::socket_t &sock1) {
    using namespace std::string_literals;
    auto [ptr, data] = prepareFn(algumacoisaFn, "Hello"s, 1, 2, 3);
    auto ptr_str = std::to_string((uint64_t)ptr);
    std::array<zmq::const_buffer, 3> send_msgs = {
        zmq::str_buffer("RPC"), zmq::buffer(ptr_str), zmq::buffer(data)};

    if (!zmq::send_multipart(sock1, send_msgs)) {
        fmt::println("Send failed");
    }
}

template <class fnT, class... Types>
auto sendRPC(zmq::socket_t &sock1, fnT fun, Types &&...args) {
    auto [ptr, data] = prepareFn(fun, std::forward<Types>(args)...);
    auto ptr_str = std::to_string((uint64_t)ptr);
    std::array<zmq::const_buffer, 3> send_msgs = {
        zmq::str_buffer("RPC"), zmq::buffer(ptr_str), zmq::buffer(data)};

    return zmq::send_multipart(sock1, send_msgs);
}

void sender(zmq::socket_t &sock1) {
    using namespace std::string_literals;
    /*sendRPC(sock1, algumacoisaFn, "Hello"s, 1, 2, 3);
    std::this_thread::sleep_for(std::chrono::seconds(1));
    sendRPC(sock1, algumacoisaFn, "World"s, 4, 5, 6);
    std::this_thread::sleep_for(std::chrono::seconds(1));
    sendRPC(sock1, algumacoisaFn, "Hello"s, 7, 8, 9);
    std::this_thread::sleep_for(std::chrono::seconds(1));
    sendRPC(sock1, algumacoisaFn, "World"s, 10, 11, 12);
    std::this_thread::sleep_for(std::chrono::seconds(1));
    sendRPC(sock1, algumacoisaFn, "Hello"s, 13, 14, 15);
    std::this_thread::sleep_for(std::chrono::seconds(1));*/
    /*std::array<zmq::const_buffer, 2> send_msgs = {zmq::str_buffer("foo"),
                                                  zmq::str_buffer("bar!")};*/
    int portno = 8080;
    struct sockaddr_in serv_addr {};
    struct sockaddr_in cli_addr {};
    socklen_t clilen{};
    struct hostent *server;
    serverfd = socket(AF_INET, SOCK_STREAM, 0);
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(portno);
    if (bind(serverfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        fmt::println("ERROR on binding");
        return;
    }

    clilen = sizeof(cli_addr);
    listen(serverfd, 5);

    while (running) {
        int newsockfd = accept(serverfd, (struct sockaddr *)&cli_addr, &clilen);
        if (newsockfd < 0 || !running) {
            break;
        }

        FdShare fdShr(newsockfd);

        sendRPC(sock1, rpcSocket, std::move(fdShr));

        /*auto fdstr = std::to_string(newsockfd);
        std::string tmpsockpath = "/tmp/sockfd_path" + fdstr;
        int unix_sock = create_unix_sock(tmpsockpath);

        fmt::println("newsockfd: {}", newsockfd);
        fmt::println("tmpsockpath: {}", tmpsockpath);

        if (unix_sock == -1) {
            fmt::println("connect_unix failed");
            return;
        }

        std::array<zmq::const_buffer, 2> send_msgs = {
            zmq::str_buffer("Connected!"), zmq::buffer(tmpsockpath)};

        if (!zmq::send_multipart(sock1, send_msgs)) {
            fmt::println("Send failed");
        }

        struct sockaddr_un cliaddr {};
        socklen_t addrlen = sizeof(cliaddr);

        int connfd = accept(unix_sock, (struct sockaddr *)&cliaddr, &addrlen);
        if (connfd < 0) {
            perror("Server accept failed");
            exit(EXIT_FAILURE);
        }

        send_fd(connfd, newsockfd);

        close(connfd);
        close(newsockfd);

        fmt::println("send_msgs[0]: {}",
                     std::string_view(
                         reinterpret_cast<const char *>(send_msgs[0].data()),
                         send_msgs[0].size()));*/
    }
}

void forkThis(zmq::socket_t &sock1) {
    if (fork() == 0) {
        fmt::println("child process: {}", getpid());
        receiveMessages();
        std::terminate();
    } else {
        fmt::println("parent process: {}", getpid());
        sender(sock1);
    }
}

void spawnNWorkers(int n) {
    for (int i = 0; i < n; i++) {
        if (fork() == 0) {
            fmt::println("child process: {}", getpid());
            receiveMessages();
            exit(0);
        }
    }
}
} // namespace

int main() {
    parentProcessId = getpid();
    signal(SIGINT, signalHandler);
    signal(SIGHUP, signalHandler);

    zmq::context_t ctx;
    zmq::socket_t sock1(ctx, zmq::socket_type::push);
    sock1.bind("tcp://127.0.0.1:*");
    last_endpoint = sock1.get(zmq::sockopt::last_endpoint);
    fmt::println("last_endpoint: {}", last_endpoint);

    spawnNWorkers(5);

    sender(sock1);

    std::this_thread::sleep_for(std::chrono::seconds(1));

    return 0;
}
