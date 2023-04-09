#include "threadpool.h"
#include "http_conn.h"


#define MAX_FD 65536   // 最大的文件描述符个数
#define MAX_EVENT_NUMBER 10000  // 监听的最大的事件数量
#define TIMESLOT 5

static int pipefd[2];
static sort_timer_lst timer_lst;

//将文件描述符设置为非阻塞的
extern int setnonblocking( int fd );
// 添加文件描述符
extern void addfd( int epollfd, int fd, bool one_shot );
//移除文件描述符
extern void removefd( int epollfd, int fd );

//将信号值发送到管道
void sig_handler( int sig )
{
    int save_errno = errno;
    int msg = sig;
    send( pipefd[1], ( char* )&msg, 1, 0 );
    errno = save_errno;
}

//添加对信号的处理函数
void addsig(int sig, void( handler )(int)){
    struct sigaction sa;
    memset( &sa, '\0', sizeof( sa ) );
    sa.sa_handler = handler;
     sa.sa_flags |= SA_RESTART;
    sigfillset( &sa.sa_mask );
    //条件返回错误，则终止程序执行
    assert( sigaction( sig, &sa, NULL ) != -1 );
}

void timer_handler(){
    // 定时处理任务，实际上就是调用tick()函数
    timer_lst.tick();
    // 因为一次 alarm 调用只会引起一次SIGALARM 信号，所以我们要重新定时，以不断触发 SIGALARM信号。
    alarm(TIMESLOT);
}

// 定时器回调函数，它删除非活动连接socket上的注册事件，并关闭之。
void cb_func( http_conn* user_data ){
    //removefd(http_conn::m_epollfd, user_data->m_sockfd);
    epoll_ctl(http_conn::m_epollfd, EPOLL_CTL_DEL, user_data->m_sockfd, 0 );
    assert( user_data );
    close( user_data->m_sockfd );
     printf( "close fd %d\n", user_data->m_sockfd );
}

int main( int argc, char* argv[] ) {
    
    if( argc <= 1 ) {
        printf( "usage: %s port_number\n", basename(argv[0]));
        return 1;
    }

    int port = atoi( argv[1] );
    //忽略 SIGPIPE 信号，防止被打断
    addsig( SIGPIPE, SIG_IGN );

    threadpool< http_conn >* pool = NULL;
    try {
        //创建线程池
        pool = new threadpool<http_conn>;
    } catch( ... ) {
        return 1;
    }

    int listenfd = socket( PF_INET, SOCK_STREAM, 0 );
    assert( listenfd >= 0 );

    int ret = 0;
    struct sockaddr_in address;
    bzero( &address, sizeof( address ) );
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_family = AF_INET;
    address.sin_port = htons( port );

    // 端口复用
    int reuse = 1;
    setsockopt( listenfd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof( reuse ) );
    ret = bind( listenfd, ( struct sockaddr* )&address, sizeof( address ) );
    assert( ret != -1 );
    ret = listen( listenfd, 5 );
    assert( ret != -1 );

    // 创建epoll对象，和事件数组，添加
    epoll_event events[ MAX_EVENT_NUMBER ];
    int epollfd = epoll_create( 5 );
    assert( epollfd != -1 );
    // 添加到epoll对象中
    addfd( epollfd, listenfd, false );
    http_conn::m_epollfd = epollfd;

    // 创建管道
    ret = socketpair(PF_UNIX, SOCK_STREAM, 0, pipefd);
    assert( ret != -1 );
    setnonblocking( pipefd[1] );
    //监听传递信号的管道
    addfd( epollfd, pipefd[0], false);

    // 设置信号处理函数
    addsig( SIGALRM , sig_handler);
    addsig( SIGTERM , sig_handler);
    bool stop_server = false;

    //创建用户请求数组
    http_conn* users = new http_conn[ MAX_FD ];
    bool timeout = false;
    alarm(TIMESLOT);  // 定时,5秒后产生SIGALARM信号

    while(true) {
        
        int number = epoll_wait( epollfd, events, MAX_EVENT_NUMBER, -1 );
        
        if ( ( number < 0 ) && ( errno != EINTR ) ) {
            printf( "epoll failure\n" );
            break;
        }

        for ( int i = 0; i < number; i++ ) {           
            int sockfd = events[i].data.fd;           
            if( sockfd == listenfd ) {
                //监听文件描述符监听到连接
                printf("can visit\n");
                struct sockaddr_in client_address;
                socklen_t client_addrlength = sizeof( client_address );
                int connfd = accept( listenfd, ( struct sockaddr* )&client_address, &client_addrlength );
                
                if ( connfd < 0 ) {
                    printf( "errno is: %d\n", errno );
                    continue;
                } 

                if( http_conn::m_user_count >= MAX_FD ) {
                    close(connfd);
                    continue;
                }
                users[connfd].init( connfd, client_address);

                // 创建定时器，设置其回调函数与超时时间，然后绑定定时器与用户数据，最后将定时器添加到链表timer_lst中
                util_timer* timer = new util_timer;
                timer->user_data = &users[connfd];
                timer->cb_func = cb_func;
                time_t cur = time( NULL );
                //3 个 TIMESLOT 为超时时间
                timer->expire = cur + 3 * TIMESLOT;

                users[connfd].timer = timer;

                timer_lst.add_timer( timer );
                printf("can visit2\n");
            }  else if( ( sockfd == pipefd[0] ) && ( events[i].events & EPOLLIN ) ) {
                // 处理信号
                printf("can sig\n");
                int sig;
                char signals[1024];
                ret = recv( pipefd[0], signals, sizeof( signals ), 0 );
                if( ret == -1 ) {
                    continue;
                } else if( ret == 0 ) {
                    continue;
                } else  {
                    for( int i = 0; i < ret; ++i ) {
                        switch( signals[i] )  {
                            case SIGALRM:
                            {
                                // 用timeout变量标记有定时任务需要处理，但不立即处理定时任务
                                // 这是因为定时任务的优先级不是很高，我们优先处理其他更重要的任务。
                                timeout = true;
                                break;
                            }
                            case SIGTERM:
                            {
                                stop_server = true;
                            }
                        }
                    }
                }
            } else if( events[i].events & ( EPOLLRDHUP | EPOLLHUP | EPOLLERR ) ) {
                //对方关闭TCP连接、错误、挂起
                util_timer* timer = users[sockfd].timer;
                users[sockfd].close_conn();
                if( timer ){
                    timer_lst.del_timer( timer );
                }

            } else if(events[i].events & EPOLLIN) {
                printf("can read\n");
                //监听文件描述符有读事件，即有请求到来
                util_timer* timer = users[sockfd].timer;
                if(users[sockfd].read()) {
                    pool->append(users + sockfd);
                    // 如果某个客户端上有数据可读，则我们要调整该连接对应的定时器，以延迟该连接被关闭的时间。
                    if( timer ) {
                        time_t cur = time( NULL );
                        timer->expire = cur + 3 * TIMESLOT;
                        printf( "adjust timer once\n" );
                        timer_lst.adjust_timer( timer );
                    }
                } else {
                    users[sockfd].close_conn();
                    if( timer ){
                        timer_lst.del_timer( timer );
                    }
                }

            }  else if( events[i].events & EPOLLOUT ) {
                //监听文件描述符有写时间，即相应内容已准备到缓冲区，等待发送
                printf("can wrtie\n");
                util_timer* timer = users[sockfd].timer;
                if( !users[sockfd].write() ) {
                    users[sockfd].close_conn();
                    if( timer ){
                        timer_lst.del_timer( timer );
                    }
                }

            }
        }

        // 最后处理定时事件，因为I/O事件有更高的优先级。当然，这样做将导致定时任务不能精准的按照预定的时间执行。
        if( timeout ) {
            timer_handler();
            timeout = false;
        }
    }
    
    close( epollfd );
    close( listenfd );
    delete [] users;
    close( pipefd[1] );
    close( pipefd[0] );
    delete pool;
    return 0;
}