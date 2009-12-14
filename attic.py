def receive_all(sock, log_prefix):
    buff = ""
    try:
        receive_size = 4096
        received = sock.recv(receive_size)
        while len(received) > 0:
            buff += received
            received = sock.recv(receive_size)
    except:
        print log_prefix, "server closed or receive timedout"
    return buff

def remote_worker_wrapper(lock, server_name, server_port):
    log_prefix = "network worker:"
    while True:
        try:
            client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_sock.connect((server_name, server_port))
            client_sock.settimeout(5)
        except:
            print log_prefix + "server is down"
            client_sock.close()
            break # end infinite loop, server no more here
        cmd = receive_all(client_sock, log_prefix)
        if cmd == "":
            client_sock.close()
            break # end infinite loop, server no more here
        #print "received:" + cmd
        cmd_out = commands.getoutput(cmd)
        try:
            to_send = parsable_echo((cmd, cmd_out))
            client_sock.sendall(to_send)
            client_sock.shutdown(socket.SHUT_WR)
            #print "sent: " + to_send
        except:
            print log_prefix + "problem while sending result"
        client_sock.close()
    #print "worker left"
    lock.release()

def server_wrapper(commands_queue, results_queue, lock, server_port):
    nb_sent_jobs     = 0
    nb_received_jobs = 0
    # start server
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock_fd = server_sock.fileno()
    server_open = True
    server_name = commands.getoutput("hostname")
    #print "server:", server_name, "port:", str(server_port)
    server_sock.bind((server_name, server_port))
    server_sock.listen(1)
    server_commands = []
    poller = select.poll()
    poller.register(server_sock, select.POLLIN) # new clients to accept
    sock_fd_dict = {} # socket to fd mapping
    while server_open:
        # sorting the poll output would give priorities to the server tasks:
        # accept new client, send job, read result, etc.
        for (fd, event) in poller.poll():
            #print "### poll"
            if fd == server_sock_fd: # accept new client
                if server_open:
                    client_conn, client_addr = server_sock.accept()
                    #print "connection from " + str(client_addr)
                    # he will want a job
                    poller.register(client_conn, select.POLLOUT)
                    sock_fd_dict[client_conn.fileno()] = client_conn
            elif event & select.POLLOUT: # send job
                try:
                    cmd = commands_queue.get(True, 1)
                    server_commands.append(cmd)
                except Empty:
                    #print "server: empty commands queue"
                    if len(server_commands) == 0:
                        #print "server: empty server queue"
                        if server_open and nb_received_jobs == nb_sent_jobs:
                            server_sock.shutdown(socket.SHUT_RDWR)
                            poller.unregister(server_sock)
                            server_sock.close()
                            server_open = False
                if len(server_commands) > 0:
                    cmd = server_commands.pop(0)
                    client_conn = sock_fd_dict[fd]
                    try:
                        client_conn.sendall(cmd)
                        client_conn.shutdown(socket.SHUT_WR)
                        poller.unregister(client_conn)
                        # he will give a result
                        poller.register(client_conn, select.POLLIN)
                        nb_sent_jobs += 1
                        #print "server: sent one job"
                        #print "nb jobs sent: " + str(nb_sent_jobs)
                    except:
                        print "server: problem while sending command"
                        server_commands.insert(0, cmd) # keep for next client
                        poller.unregister(client_conn)
                        client_conn.close() # fire client
            elif event & select.POLLIN: # get result
                client_conn = sock_fd_dict[fd]
                result = receive_all(client_conn, "server:")
                #print "server received:" + result
                poller.unregister(client_conn)
                client_conn.close()
                sock_fd_dict.pop(fd)
                nb_received_jobs += 1
                results_queue.put(parse_cmd_echo(result))
                #print "server: got one job"
                #print "nb jobs received: " + str(nb_received_jobs)
            else:
                print "server: unhandled event"
                sys.exit(1)
    #print "server: all jobs done"
    lock.release()
