#ifndef _WIN32
#include <poll.h>
#else
#include <WinSock2.h>
#endif

#include <stdlib.h>
#include <stdio.h>
#include <mysql.h>


int wait_for_mysql(MYSQL *mysql, int status) {
#ifdef _WIN32
    fd_set rs, ws, es;
    int res;
    struct timeval tv, *timeout;

    my_socket s= mysql_get_socket(mysql);
    FD_ZERO(&rs);
    FD_ZERO(&ws);
    FD_ZERO(&es);

    if(status & MYSQL_WAIT_READ)
        FD_SET(s, &rs);
    if(status & MYSQL_WAIT_WRITE)
        FD_SET(s, &ws);
    if(status & MYSQL_WAIT_EXCEPT)
        FD_SET(s, &es);
    if(status & MYSQL_WAIT_TIMEOUT) {
        tv.tv_sec = mysql_get_timeout_value(mysql);
        tv.tv_usec = 0;
        timeout = &tv;
    } else {
        timeout = NULL;
    }

    res = select(1, &rs, &ws, &es, timeout);
    if(res == 0) {
        return MYSQL_WAIT_TIMEOUT;
    } else if(res == SOCKET_ERROR) {
        return MYSQL_WAIT_TIMEOUT;
    } else {
        int status= 0;
        if(FD_ISSET(s, &rs))
            status |= MYSQL_WAIT_READ;
        if(FD_ISSET(s, &ws))
            status |= MYSQL_WAIT_WRITE;
        if(FD_ISSET(s, &es))
            status |= MYSQL_WAIT_EXCEPT;
        return status;
    }
#else
    struct pollfd pfd;
    int timeout;
    int res;

    pfd.fd= mysql_get_socket(mysql);
    pfd.events=
        (status & MYSQL_WAIT_READ ? POLLIN : 0) |
        (status & MYSQL_WAIT_WRITE ? POLLOUT : 0) |
        (status & MYSQL_WAIT_EXCEPT ? POLLPRI : 0);

    if(status & MYSQL_WAIT_TIMEOUT) {
        timeout = 1000 * mysql_get_timeout_value(mysql);
    } else {
        timeout = -1;
    }

    res = poll(&pfd, 1, timeout);
    if(res == 0) {
        return MYSQL_WAIT_TIMEOUT;
    } else if(res < 0) {
        return MYSQL_WAIT_TIMEOUT;
    } else {
        int status= 0;
        if (pfd.revents & POLLIN)
            status|= MYSQL_WAIT_READ;
        if (pfd.revents & POLLOUT)
            status|= MYSQL_WAIT_WRITE;
        if (pfd.revents & POLLPRI)
            status|= MYSQL_WAIT_EXCEPT;
        return status;
    }
#endif
}
