/*!
 * \defgroup  tcp_sock 
 * \author Anup Pemmaiah
 *
 * \section Purpose
 * High performance lockless TCP receiver to handle any kind of disparate
 * data
 */

#include <stdlib.h>
#include <sys/epoll.h>
#include <sys/resource.h>

/*
 * TDSR TCP Flags
 */
#define	TDSR_TCP_SGL_THR 0x0001
#define TDSR_TCP_MSG_TIMER_EXP 0x0010

#define DEF_NUM_POLL_THR 2
#define DEF_LISTEN_Q 50
#define DEF_MAX_EPOLL_EVS 1024 
#define DEF_RECV_BUF_SZ 512

enum _ctrl_cmds {
	INIT_DONE
};

/*!
 * \struct ctrl_cmd_st_t
 * Structure for communication over the control bus
 */
typedef struct _ctrl_cmd_st {
	int ctrl_cmd;
	void *arg;
}ctrl_cmd_st_t;

/*!
 * \struct poll_thr_info_t
 * Polling thread info
 */
typedef struct _poll_thr_info {
	pthread_t tid;		/*!< Thread id of the polling thread */
	void *sock;     	/*!< Sock the thread belongs to */
	int epoll_fd;		/*!< Epoll fd used by the polling thr */
	int num_con_fds;	/*!< Num of connections assigned to the polling thr */
	size_t recv_buf_sz;	/*!< Size of the recv'g buf */
	int cbq_tid;		/*!< Con id of this thread to CBQ */
	int pctrl_bus[2];	/*!< The bus to send ctrl cmds between threads. bus[0]*/
						/*!< = main thr, bus[1] = polling thr */
	struct _poll_thr_info *next;
}poll_thr_info_t;


/*!
 * \struct recv_thr_cb_info_t
 * Track the receiving threads that is using the CB
 * NOTE:-
 * Receiving thread pops a CB from CBQ and pushes back the CB into CBQ after
 * a message is read. Therefore it connects to CBQ both as a consumer and a
 * producer. The conumer ID is used as the connection ID by the TDSR TCP API's
 */
typedef struct _recv_thr_cb_info {
	pthread_t pth_id;	/*! ID of the thread */
	void *cb;			/*! The con buffer the thread is using */
	int	con_id;			/*! Con ID(CBQ cons ID). Array is idx'd based on this */
	int prod_id;		/*! CBQ prod ID */
}recv_thr_cb_info_t;
 

/*!
 * \struct tdsr_tcp_cbq_t
 * The struct of  CBQ
 */
typedef struct _tdsr_tcp_cbq {
	void  *cb_addr_q;						/*! CB address queue */ 
	recv_thr_cb_info_t *recv_thr_cb_arr;	/*! Receiving thread CB tracker */
}tdsr_tcp_cbq_t;

/*!
 * \struct tdsr_tcp_t
 * The TDSR TCP SOCKET
 */
typedef struct _tdsr_tcp {
	char *name;					/*!< Name fof the socket */
	uint32_t port;				/*!< Port for the socket */
	int num_poll_thr;			/*!<Num of polling threads  */
	void **conn_buf_arr;	/*!< Array of CB addr of the sock idx'd by con_fd */
	int max_fd_rlimit;		/*!< Current max fd value resource limit */
	tdsr_tcp_cbq_t *cbq;	/*!< CBQ */
	//ctrl_pipe_info_t *ctrl_pipe;		/*!< Control pipe */
	poll_thr_info_t  *poll_thr_list;	/*!< Head of  polling thread list */
	//poll_thread_info_t  *poll_thr_tracker;	/*!< Poll thread tracker */
	char *nw_config; 			/*!< The socket specific tdsr config */
	int listen_fd;              /*!< Main fd of the sock to listen */
	int main_epoll_fd;			/*!< Main epoll fd */
	unsigned long push_tot_sz; /*!< Total data size pushed into sock for stat */
	unsigned long pop_tot_sz; /*!< Total data size poped from sock for stat */
	int mctrl_bus[2];	/*!< The bus to send ctrl cmds between threads. bus[0]*/
						/*!< = tcp API thr, bus[1] = main thr */
}tdsr_tcp_t;

static void *tdsr_tcp_main_thr(void *arg);
static int tdsr_tcp_con_init(tdsr_tcp_t *self, int con_fd, json_object *nw_obj);
static int tdsr_tcp_con_close(tdsr_tcp_t *self, int con_fd, void *thr_info);
static int tdsr_tcp_add_thr_list(void **list_head, void *thr_info);
//static int tdsr_tcp_del_thr_list(void **list_head, void *thr_info);
static int tdsr_tcp_clear_thr_list(void **list_head);


/*!
 * Add a polling thr info to the list
 * \param list_head: Head of the list
 * \param thr_info: Pointer to the polling thr info
 * \return: 0 on success
 *			-1 on failure
 */
static int
tdsr_tcp_add_thr_list(void **list_head, void *thr_info) {

	poll_thr_info_t *head;
	poll_thr_info_t *tinfo = (poll_thr_info_t *)thr_info;

	if (!tinfo || !list_head) {
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Invalid args to add into the "
				 "polling thr list");
		return (-1);
	}

	head = (poll_thr_info_t *)(*list_head);

	if (!head) {
		*list_head = tinfo;
		return (0);
	}

	while(head->next) {
		head = head->next;
	}
	head->next = tinfo;

	return (0);
}


/*!
 * Delete the polling thread info from the list
 * \param list_head: Head of the list
 * \param thr_info: Pointer to the polling thr info 
 * \return: 0 on success
 *			-1 on failure
 */
#if 0
static int
tdsr_tcp_del_thr_list(void **list_head, void *thr_info) {

	poll_thr_info_t *head, *prev_tinfo;
	poll_thr_info_t *tinfo = (poll_thr_info_t *)thr_info;

	if (!tinfo || !list_head) {
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Invalid args to del from the "
				 "polling thr list");
		return (-1);
	}

	head = (poll_thr_info_t *)(*list_head);

	if (head == tinfo) {
		*list_head = head->next;
		return (0);
	}

	while(head) {
		prev_tinfo = head;
		head = head->next;
		if (head == tinfo) {
			prev_tinfo->next = head->next;
			return (0);
		}
	}
	tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Could not find polling thr in the "
			 "list to del");
	return (0);
}
#endif

/*!
 * Clear the entire polling thread list
 * \param list_head: Head of the list
 * \return: 0 on success
 *			-1 on failure
 */
static int
tdsr_tcp_clear_thr_list(void **list_head) {

	poll_thr_info_t *head, *next_tinfo;

	if (!list_head) {
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Invalid args to clear from the "
				 "polling thr list");
		return (-1);
	}

	head = (poll_thr_info_t *)(*list_head);

	while(head) {
		next_tinfo = head->next;
		free(head);
		head = next_tinfo;
	}
	return (0);
}

/*!
 * Recv data from a connection
 * \param self: Pointer to the TDSR TCP socket
 * \param con_fd: Connection to recv data from
 * \param recv_buf: Buffer to read data into
 * \param recv_buf_sz: Buffer size
 * \return: 0 on Success
 *			-1 internal error
 *			error code for a system error
 */
static inline int
con_recv_data(int con_fd, void *recv_buf, void *thr_info) {

	/*
	 * Read data from client connections
	 */
	ssize_t recv_sz;
	int res;
	void *cb = NULL;
	size_t recv_buf_sz;
	void *cb_addr_q;
	int cbq_tid;
	pthread_t pth_id;
	tdsr_tcp_t *self;
	poll_thr_info_t *tinfo = (poll_thr_info_t *)thr_info;

	if (!recv_buf || !tinfo) {
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Invalid args to recv con data");
		return (-1);
	}

	self = (tdsr_tcp_t *)tinfo->sock;
	recv_buf_sz = tinfo->recv_buf_sz;
	cb_addr_q = self->cbq->cb_addr_q;
	pth_id = tinfo->tid;
	cbq_tid = tinfo->cbq_tid;

	cb = self->conn_buf_arr[con_fd];
	if (!cb) {
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Could not find a CB "
			 "for the incomming data from con (%d) at port %d for "
			"tcp socket %s",con_fd, self->port, self->name); 
		return(-1);
	}
	

	recv_sz = recv(con_fd, recv_buf, recv_buf_sz, 0);
	if (recv_sz > 0) {
		res = tdsr_cb_push(cb, recv_buf, recv_sz, con_fd);
		if (CB_ERR_OK != res) {
			/*
			 * Perform the required error handling and HWM
			 * monitoring
			 */
			tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Error pushing data "
					 "into CB at port %d for tcp socket %s (%d)",
					 self->port, self->name, res);
			tdsr_tcp_con_close(self, con_fd, thr_info);
		} else {
			/*
			 * If is_empty flag is set, push the CB into CBQ
			 */
			__sync_fetch_and_add(&(self->push_tot_sz), recv_sz);
			if (tdsr_cb_get_is_empty(cb)) {
				tdsr_cb_set_is_empty(cb, false);
				res = tipc_msock_push(cb_addr_q, cbq_tid, pth_id, cb);
				if (TIPC_ERR_OK != res) {
					tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Error pushing"
						 " CB into CBQ at port %d for tcp socket %s "
						 "(%d). Closing the connection",
						 self->port, self->name, res);
					tdsr_tcp_con_close(self, con_fd, thr_info);
				} else {
					//tdsr_log(TYPE_SYSLOG_ONLY, LOG_INFO, "Pushed CB "
					//	" into CBQ at port %d for tcp socket %s ",
					//	self->port, self->name);
				}
			}
		}
		
	} else if (0 == recv_sz) {
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Connection close recv'd "
					 "for con %d at port %d for tcp socket %s",
					 con_fd, self->port, self->name);
		tdsr_tcp_con_close(self, con_fd, thr_info);
	} else if (-1 == recv_sz) {
		return (errno);
	}
	return (0);
}

/*!
 * Connection related polling thread
 * \param arg: Pointer to poll_thr_arg
 */
static void *
con_polling_thr(void *arg) {

	poll_thr_info_t *thr_info;
	int epoll_fd;
	tdsr_tcp_t *self;
	int max_evs;
	int nfds;
	struct epoll_event *poll_evs;
	size_t recv_buf_sz;
	void *recv_buf = NULL;
	pthread_t pth_id;
	int cbq_tid;
	int cbq_err_code;
	void *cb_addr_q;

	if (!arg) {
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR,
				 						"Invalid arg in the polling thread");
		return (NULL);
	}

	thr_info = (poll_thr_info_t *)arg;
	epoll_fd = thr_info->epoll_fd;
	self = (tdsr_tcp_t *)thr_info->sock;
	cb_addr_q = self->cbq->cb_addr_q;


	max_evs = DEF_MAX_EPOLL_EVS;
	poll_evs = (struct epoll_event *)calloc(max_evs,sizeof(struct epoll_event));
	if (!poll_evs) {
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Error aloc'g mem for poll evs "
				"at port %d for tcp socket %s (%d)", 
				 self->port, self->name, errno);
		goto con_poll_thr_err;
	}

	/*
	 * Connect this thread to CBQ as producer
	 */
	pth_id = pthread_self();
	thr_info->tid = pth_id;
	cbq_tid = tipc_msock_connect(cb_addr_q, TIPC_CON_TYPE_PROD, pth_id,
								 							&cbq_err_code);
	if (-1 == cbq_tid) {
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Could not connect polling thr to "
				 "CBQ at port %d for tcp socket %s (%d)",
				 self->port, self->name, cbq_err_code);
		goto con_poll_thr_err;
	}
	thr_info->cbq_tid = cbq_tid;

	/*
	 * Create a recv buf
	 */
	recv_buf_sz = thr_info->recv_buf_sz;
	recv_buf = calloc(recv_buf_sz, 1);
	if (!recv_buf) {
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Error alloc'g mem for recv buf "
				 "at port %d for tcp socket %s (%d)",
				 self->port, self->name, errno);
		goto con_poll_thr_err;
	}

	/*
	 * Send a INIT_DONE to the waiting main thread
	 */
	ctrl_cmd_st_t cmd_st;
	cmd_st.ctrl_cmd = INIT_DONE;
	cmd_st.arg = NULL;
	if (write(thr_info->pctrl_bus[1], &cmd_st, sizeof(ctrl_cmd_st_t)) < 0) {
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Error sending init done from the "
				 "polling thr to the main thr at port %d for tcp socket %s (%d)"
				 , self->port, self->name, errno);
		goto con_poll_thr_err;
	}
	tdsr_log(TYPE_SYSLOG_ONLY, LOG_INFO, "Sent polling thr init done at port "
			 "%d for tcp socket %s", self->port, self->name);

	while (1) {
		int n;

		nfds = epoll_wait(epoll_fd, poll_evs, max_evs, 0);
		if (-1 == nfds) {
			tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "poll thr polling err at port "
					 "%d for tcp socket %s (%d)",self->port, self->name, errno);
			goto con_poll_thr_err;
		}

		for(n = 0; n < nfds; ++n) {
			int res;
			int con_fd = poll_evs[n].data.fd;

			res = con_recv_data(con_fd, recv_buf, thr_info);
			if (res == -1) {
				goto con_poll_thr_err;
			} else if (res > 0) {
				if ((res == EAGAIN) || (res == EWOULDBLOCK)) {
					continue;
				}
				tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Error recv'g data "
							 "for con %d at port %d for tcp socket %s (%d). "
							 "Closing the connection",
							 con_fd, self->port, self->name, res);
				tdsr_tcp_con_close(self, con_fd, thr_info);
			}
		}
	}

con_poll_thr_err:

	if (poll_evs) {
		free(poll_evs);
	}
	if (recv_buf) {
		free(recv_buf);
	}
	return (NULL);
}

/*!
 * New client connection specific initialization
 * \param self: TDSR TCP sock
 * \param con_fd: Connection fd
 * \return: 0 on success
 *			-1 on failure
 */
static int
tdsr_tcp_con_init(tdsr_tcp_t *self, int con_fd, json_object *nw_obj) {

	void *cb = NULL;
	char *cb_sz;
	int cb_wr_tout_cnt;
	int cb_rd_tout_cnt;

	if (con_fd > self->max_fd_rlimit) {
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Value of con fd (%d) is greater "
				 "than the max fd val (%d) for sock %s at port %d",
				 con_fd, self->max_fd_rlimit, self->name, self->port);
		return (-1);
	}

	JOBJ_GET_STR_NO_RET(nw_obj, "cb_sz", cb_sz);

	JOBJ_GET_INT_NO_RET(nw_obj, "cb_wr_tout_count", cb_wr_tout_cnt);

	JOBJ_GET_INT_NO_RET(nw_obj, "cb_rd_tout_count", cb_rd_tout_cnt);

	/*
	 * Create a new CB
	 */
	cb = tdsr_cb_create(self, cb_sz, con_fd);
	if (!cb) {
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Error creating CB for sock %s "
				 "at port %d", self->name, self->port);
		return (-1);
	}

	if (cb_wr_tout_cnt > 0) {
		tdsr_cb_set_wr_tout_cnt(cb, cb_wr_tout_cnt);
	}

	if (cb_rd_tout_cnt > 0) {
		tdsr_cb_set_rd_tout_cnt(cb, cb_rd_tout_cnt);
	}

	self->conn_buf_arr[con_fd] = cb;

	/*
	 * Submit this connection to one of the polling threads with least 
	 * connections 
	 */
	poll_thr_info_t *tinfo = self->poll_thr_list;
	poll_thr_info_t *con_tinfo = tinfo;
	int least_fds = -1;
	while (tinfo) {
		if (-1 == least_fds) {
			least_fds = tinfo->num_con_fds;
			con_tinfo = tinfo; 
		} else {
			if (least_fds > tinfo->num_con_fds) {
				least_fds = tinfo->num_con_fds;
				con_tinfo = tinfo;
			}
		}
		tinfo = tinfo->next;
	}
	if (!con_tinfo) {
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Could not find a con polling "
				 "thread to submit the new connection for sock %s at port %d",
				 self->name, self->port);
		tdsr_cb_destroy(&cb);
		self->conn_buf_arr[con_fd] = NULL;
		return (-1);
	}

	struct epoll_event ev;
	ev.events = EPOLLIN;
	ev.data.fd = con_fd;
	if (epoll_ctl(con_tinfo->epoll_fd, EPOLL_CTL_ADD, con_fd, &ev) == -1) {
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Error adding  con to poll at port "
				 "%d for tcp socket %s (%d)", self->port, self->name, errno);
		tdsr_cb_destroy(&cb);
		self->conn_buf_arr[con_fd] = NULL;
		return (-1);
	}

	con_tinfo->num_con_fds++;	
	return (0);
}

/*!
 * Close client connection
 * \param self: TDSR TCP sock
 * \param con_fd: Connection fd
 * \return: 0 on success
 *			-1 on failure
 */
static int
tdsr_tcp_con_close(tdsr_tcp_t *self, int con_fd, void *thr_info) {

	void *cb = NULL;
	poll_thr_info_t *tinfo = (poll_thr_info_t *)thr_info;

	if (!self || (con_fd <= 0)) {
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Invalid args to close tdsr "
				 "tcp con");
		return (-1);
	}

	/*
	 * Get the thread info handling the connection
	 */
	if (tinfo) {
		if (epoll_ctl(tinfo->epoll_fd, EPOLL_CTL_DEL, con_fd, NULL) == -1) {
			tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Error deleting con_fd %d from"
					" poll at port %d for tcp socket %s (%d)",
					con_fd, self->port, self->name, errno);
		}
	} else {
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Could not find the thread info "
				 "to close the conn for for con_fd %d at port %d for tcp socket"
				 " %s", con_fd, self->port, self->name);
	}
	
	close(con_fd);

	/*
	 * Do all the clean up
	 */

	cb = self->conn_buf_arr[con_fd];
	if (cb) {
		/*
		 * Set CB conn closed flag to true. It will get destroyed once the CB
		 * gets empty
		 */
		tdsr_cb_set_is_con_closed(cb, true);
	}
	self->conn_buf_arr[con_fd] = NULL;
	tinfo->num_con_fds--;

	return (0);
}

/*!
 * Main thread of the TDSR TCP socket
 * \param arg : The TDSR TCP Sock handle
 */
static void *
tdsr_tcp_main_thr(void *arg) {

	tdsr_tcp_t *self = (tdsr_tcp_t *)arg;	
	json_object *nw_obj;
	int listen_fd = 0;
	int reuse = 1;
	int res = 0;
	int main_epoll_fd;
	struct epoll_event ev;
	struct epoll_event	*main_evs = NULL;
	int max_evs;
	int nfds;
	struct sockaddr_in6 serv_addr;
	char *str_cbq_sz;
	char *str_recv_buf_sz;
	void *main_recv_buf = NULL;
	size_t main_recv_buf_sz;
	poll_thr_info_t *main_poll_thr_info = NULL;

	if (!self) {
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Invalid argument to TDSR TCP "
				 "main thread");
		return (NULL);
	}

	/*
	 * Get the config settings
	 */
	nw_obj = json_tokener_parse(self->nw_config);
	if (!nw_obj) {
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Error parsing tdsr tcp config for"
				 " %s", self->name);
		goto main_thr_clean;
	}

	JOBJ_GET_INT_NO_RET(nw_obj, "port", self->port);
	if (self->port <= 0) {
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Need a valid port for tcp socket"
				 " %s", self->name);
		goto main_thr_clean;
	}

	JOBJ_GET_INT_NO_RET(nw_obj, "num_poll_threads", self->num_poll_thr);
	if (self->num_poll_thr < 0) {
		self->num_poll_thr = DEF_NUM_POLL_THR;
	}

	JOBJ_GET_STR_NO_RET(nw_obj, "recv_buf_sz", str_recv_buf_sz);

	JOBJ_GET_STR_NO_RET(nw_obj, "cbq_sz", str_cbq_sz);

	/*
	 * Initialize the port
	 */
	listen_fd = socket(AF_INET6, SOCK_STREAM | SOCK_NONBLOCK, 0);
	if (-1 == listen_fd) {
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Error creating socket at port %d"
				 " for tcp socket %s (%d)", self->port, self->name, errno);
		goto main_thr_clean;
	}
	self->listen_fd = listen_fd;
//---------------TESTING------------
	int sock_val;
	unsigned int sock_val_sz = sizeof(sock_val);
	res = getsockopt(listen_fd, SOL_SOCKET, SO_RCVBUF, &sock_val, &sock_val_sz);
	if (-1 == res) {

		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Error getting buf sz of socket at "
				 "port %d for tcp socket %s (%d)",
				 self->port, self->name, errno);
	} else {
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_INFO, "Sock Buf Size: %d", sock_val);
	}
#if 0 
	//sock_val = (128 * 1024 *1024);
	sock_val = 212992;
	res = setsockopt(listen_fd, SOL_SOCKET, SO_RCVBUFFORCE, &sock_val, sock_val_sz);
	if (-1 == res) {

		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Error setting buf sz of socket at "
				 "port %d for tcp socket %s (%d)",
				 self->port, self->name, errno);
	}
	res = getsockopt(listen_fd, SOL_SOCKET, SO_RCVBUF, &sock_val, &sock_val_sz);
	if (-1 == res) {

		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Error getting new buf sz of socket"
				 " at port %d for tcp socket %s (%d)",
				 self->port, self->name, errno);
	} else {
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_INFO, "New Sock Buf Size: %d", sock_val);
	}
#endif	
//----------------------------------

	/*
	 * Allow the local addr to be reused when TDSR restarts before wait time
	 * expires to avoid errors like "addr already bound...." etc.
	 */
	res = setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &reuse,sizeof(reuse));
	if (-1 == res) {

		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Error reusing addr of socket at "
				 "port %d for tcp socket %s (%d)",
				 self->port, self->name, errno);
	}

	/*
     * Support both IPV4 and IPV6
	 */	 
	memset(&serv_addr, '0', sizeof(serv_addr));

	serv_addr.sin6_family = AF_INET6;
	serv_addr.sin6_addr = in6addr_any;	
	serv_addr.sin6_port = htons(self->port);

	res = bind(listen_fd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)); 
	if (-1 == res) {
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Error binding socket at "
				 "port %d for tcp socket %s (%d)",
				 self->port, self->name, errno);
		goto main_thr_clean;
	}


	/*
	 * Start listening
	 */
	res = listen(listen_fd, DEF_LISTEN_Q);
	if (-1 == res) {
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Error listening socket at "
				 "port %d for tcp socket %s (%d)",
				 self->port, self->name, errno);
		goto main_thr_clean;
	}

	/*
     * CBQ related init
	 */	 
	self->cbq = (tdsr_tcp_cbq_t *)calloc(1, sizeof(tdsr_tcp_cbq_t));
	if (!self->cbq) {
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Could not alloc mem to cbq at "
			"port %d for tcp socket %s (%d)", self->port, self->name, errno);
		goto main_thr_clean;
	}

	int err_code;
	char cbq_name[128];
	size_t cbq_sz = get_sz_from_str(str_cbq_sz);
	snprintf(cbq_name, sizeof(cbq_name), "%s_%d_cbq", self->name, self->port);
	self->cbq->cb_addr_q = tipc_msock_init(cbq_name, TIPC_MSG_TYPE_CB, 0,cbq_sz,
															&err_code);
	if (!self->cbq->cb_addr_q) {
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Error creating CBQ at port %d "
				 "for tcp socket %s (%d)", self->port, self->name, err_code);
		goto main_thr_clean;
	}

	int cbq_thr_lim = tipc_msock_get_thr_lim(self->cbq->cb_addr_q);
	if (-1 == cbq_thr_lim) {
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Could not get CBQ max thr lim at"
				 "port %d for tcp socket %s", self->port, self->name);
		goto main_thr_clean;
	}
	self->cbq->recv_thr_cb_arr = calloc(cbq_thr_lim,sizeof(recv_thr_cb_info_t));
	if (!self->cbq->recv_thr_cb_arr) {
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Error alloc'g mem for recv thr CB "
				 "info array with max cbq thread lim: %d in socket %s: %d",
				 cbq_thr_lim, self->name, errno);
		goto main_thr_clean;
	}

	/*
	 * Perform main thread functionalities like looking out for new connections,
	 * other maintenance and bookeeping work
	 */
	main_epoll_fd = epoll_create(EPOLL_CLOEXEC);
	if (-1 == main_epoll_fd) {
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Error creating main epoll at port "
				 "%d for tcp socket %s (%d)", self->port, self->name, errno);
		goto main_thr_clean;
	}
	self->main_epoll_fd = main_epoll_fd;

	ev.events = EPOLLIN;
	ev.data.fd = listen_fd;
	res = epoll_ctl(main_epoll_fd, EPOLL_CTL_ADD, listen_fd, &ev);
	if (-1 == res) {
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Error adding  main poll at port "
				 "%d for tcp socket %s (%d)", self->port, self->name, errno);
		goto main_thr_clean;
	}

	max_evs = DEF_MAX_EPOLL_EVS;
	main_evs = (struct epoll_event *)calloc(max_evs,sizeof(struct epoll_event));
	if (!main_evs) {
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Error aloc'g mem for main evs "
				"at port %d for tcp socket %s (%d)", 
				 self->port, self->name, errno);
		goto main_thr_clean;
	}

	/*
	 * Polling thread related init
	 */
	if (self->num_poll_thr > 0) {
		/*
   		 * Create polling threads
		 */
		pthread_t tid;
		int thr_epoll_fd;
		poll_thr_info_t *thr_info;

		for (int i = 0; i < self->num_poll_thr; i++) {
			/*
			 * Create a epoll fd for the polling thread
			 */
			thr_epoll_fd = epoll_create(EPOLL_CLOEXEC);
			if (-1 == thr_epoll_fd) {
				tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Error creating polling thr"
					" epoll at port %d for tcp socket %s (%d)", 
				 	self->port, self->name, errno);
				goto main_thr_clean;
			}

			/*
			 * Create a new polling thread info
			 */
			thr_info = (poll_thr_info_t *) calloc(1, sizeof(poll_thr_info_t));
			if (!thr_info) {
				tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Error alloc'g mem for a "
						 "new poll thr info at port %d for tcp socket %s (%d)",
						 self->port, self->name, errno);
				goto main_thr_clean;
			}
			thr_info->epoll_fd = thr_epoll_fd;
			thr_info->sock = self;
			thr_info->recv_buf_sz = get_sz_from_str(str_recv_buf_sz);
			if (0 == thr_info->recv_buf_sz) {
				thr_info->recv_buf_sz = DEF_RECV_BUF_SZ;
			}

			/*
			 * Create the control bus between the main thr and polling thr
			 */
			if (socketpair(AF_UNIX, SOCK_STREAM, 0, thr_info->pctrl_bus) < 0) {
				tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Error creating ctrl bus "
						 "for polling thr at port %d for tcp socket %s (%d)",
						 self->port, self->name, errno);
				goto main_thr_clean;
			}

			res = pthread_create(&tid, NULL, &con_polling_thr, thr_info);
			if (0 != res) {
				tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Error creating tdsr tcp "
						 "polling thr at port %d for tcp socket %s (%d)",
						 self->port, self->name, errno);
				goto main_thr_clean;
			}
			/*
			 * Wait till the polling thread is initialized
			 */
			tdsr_log(TYPE_SYSLOG_ONLY, LOG_INFO, "Waiting for polling thread "
					 "%d to initialize at port %d for tcp socket %s",
					i, self->port, self->name);
			ctrl_cmd_st_t ctrl_msg;
			if (read(thr_info->pctrl_bus[0], &ctrl_msg, sizeof(ctrl_msg)) < 0) {
				tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Error waiting for the "
				 	"polling thread %d to initialize at port %d for tcp "
					"socket %s (%d)", i, self->port, self->name, errno);
				 goto main_thr_clean;
			}

			if (ctrl_msg.ctrl_cmd != INIT_DONE) {
				tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Expected init done cmd "
					 "for polling thread %d to initialize at port %d for tcp "
					 " socket %s, but received cmd %d",
					 i, self->port, self->name, ctrl_msg.ctrl_cmd);
				goto main_thr_clean;
			}	
		
			tdsr_log(TYPE_SYSLOG_ONLY, LOG_INFO, "Done Waiting for polling "
					 "thread %d to initialize at port %d for tcp socket %s",
					i, self->port, self->name);

			/*
			 * Add the new polling thread info to the list
			 */
			//thr_info->tid = tid;
			tdsr_tcp_add_thr_list((void **)&(self->poll_thr_list), thr_info);
		}
	} else {
		/* 
		 * In case of no polling threads, its functionalities will be done
		 * by the main thr
		 */
		pthread_t pth_id;
		int cbq_tid;
		void *cb_addr_q;
		int cbq_err_code;

		tdsr_log(TYPE_SYSLOG_ONLY, LOG_INFO, "------- main thr takes care of polling");

		/*
		 * Create a new polling thread info
		 */
		main_poll_thr_info = (poll_thr_info_t *) calloc(1,
													sizeof(poll_thr_info_t));
		if (!main_poll_thr_info) {
			tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Error alloc'g mem for a "
					 "new single thr info at port %d for tcp socket %s (%d)",
					 self->port, self->name, errno);
			goto main_thr_clean;
		}
		main_poll_thr_info->epoll_fd = main_epoll_fd;
		main_poll_thr_info->sock = self;
		main_poll_thr_info->recv_buf_sz = get_sz_from_str(str_recv_buf_sz);
		if (0 == main_poll_thr_info->recv_buf_sz) {
			main_poll_thr_info->recv_buf_sz = DEF_RECV_BUF_SZ;
		}
		tdsr_tcp_add_thr_list((void **)&(self->poll_thr_list),
							  							main_poll_thr_info);

		/*
		 * Connect this thread to CBQ as producer
		 */
		pth_id = pthread_self();
		main_poll_thr_info->tid = pth_id;
		cb_addr_q = self->cbq->cb_addr_q;

		cbq_tid = tipc_msock_connect(cb_addr_q, TIPC_CON_TYPE_PROD, pth_id,
								 							&cbq_err_code);
		if (-1 == cbq_tid) {
			tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Could not connect main polling"
				 " thr to CBQ at port %d for tcp socket %s (%d)",
					 self->port, self->name, cbq_err_code);
			goto main_thr_clean;
		}
		main_poll_thr_info->cbq_tid = cbq_tid;

		/*
		 * Create a recv buf
		 */
		main_recv_buf_sz = main_poll_thr_info->recv_buf_sz;
		main_recv_buf = calloc(main_recv_buf_sz, 1);
		if (!main_recv_buf) {
			tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Error alloc'g mem for recv buf"
					 " in main thr at port %d for tcp socket %s (%d)",
					 self->port, self->name, errno);
			goto main_thr_clean;
		}
	}	

	/*
	 * Send a INIT_DONE to the waiting socket thread
	 */
	ctrl_cmd_st_t cmd_st;
	cmd_st.ctrl_cmd = INIT_DONE;
	cmd_st.arg = NULL;
	if (write(self->mctrl_bus[1], &cmd_st, sizeof(ctrl_cmd_st_t)) < 0) {
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Error sending init done from the "
				 "main thr to the sock thr at port %d for tcp socket %s (%d)"
				 , self->port, self->name, errno);
		goto main_thr_clean;
	}
	tdsr_log(TYPE_SYSLOG_ONLY, LOG_INFO, "Sent main thr init done at port "
			 "%d for tcp socket %s", self->port, self->name);

	while(1) {
		int n;
		int con_fd;
		struct sockaddr_in6 cl_addr;
		socklen_t addr_len;
		char cl_str_addr[INET6_ADDRSTRLEN];

		nfds = epoll_wait(main_epoll_fd, main_evs, max_evs, 0);
		if (-1 == nfds) {
			tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "main polling err at port %d ",
					 "for tcp socket %s (%d)",self->port, self->name, errno);
			goto main_thr_clean;
		}

		for(n = 0; n < nfds; ++n) {
			int rfd = main_evs[n].data.fd;

			if (rfd == listen_fd) {
				addr_len = sizeof(cl_addr);
				/*
				 * ----------FIX ME---------
				 * Provide a flag, if set to not accept new connections
				 * in cases where socket is hung, or buffers are full etc
				 */
				con_fd = accept(listen_fd, (struct sockaddr *)&cl_addr, 
																	&addr_len);
				if (-1 == con_fd) {
					tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Error accepting a"
							 "connection at port %d for tcp socket %s (%d)",
							 self->port, self->name, errno);
					continue;
				}
				/*
				 * Print the client info
				 */
				if (inet_ntop(AF_INET6, &cl_addr.sin6_addr, cl_str_addr, 
							  sizeof(cl_str_addr))) {
					tdsr_log(TYPE_SYSLOG_AND_CONS, LOG_INFO, "Client Addr: %s"
							": %d", cl_str_addr, ntohs(cl_addr.sin6_port)); 
				}

				/*
				 * Connection specific init
				 */
				if (tdsr_tcp_con_init(self, con_fd, nw_obj) != 0) {
					tdsr_log(TYPE_SYSLOG_AND_CONS, LOG_ERR, "Error performing"
							 " connection init at port %d for tcp socket %s",
							 self->port, self->name);
				}
			} else {
				/*
				 * It is client data from one of the connections
				 */
				res = con_recv_data(rfd, main_recv_buf, main_poll_thr_info);
				if (res == -1) {
					goto main_thr_clean;
				} else if (res > 0) {
					if ((res == EAGAIN) || (res == EWOULDBLOCK)) {
						continue;
					}
					tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Error recv'g data "
							 "for con %d at port %d for tcp socket %s (%d). "
							 "Closing the connection",
							 rfd, self->port, self->name, res);
					tdsr_tcp_con_close(self, rfd, main_poll_thr_info);
				}
			}
		}
	}

main_thr_clean:
	
	if (main_evs) {
		free(main_evs);
	}
	if (main_recv_buf) {
		free(main_recv_buf);
	}
	tdsr_tcp_destroy((void **)&self);
	return (NULL);
}


/*!
 * Create a new TDSR TCP socket
 * \param name: Name of the socket
 * \port: Port used by the socket
 * \flag: TDSR TCP specific flags
 * \return socket on success
 *		   NULL on failure
 */
void *
tdsr_tcp_new(char *name, json_object *nw_obj) {

	tdsr_tcp_t *self = NULL;
	int res = 0;
	pthread_t main_thr_id;

	if (!name || !nw_obj) {
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Invalid arguments to create a "
				 "new TDSR TCP socket");
		goto new_clean;
	}

	self = (tdsr_tcp_t *)calloc(1, sizeof(tdsr_tcp_t));
	if (!self) {
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Error allocating memory to create"
				 " new TDSR TCP socket %s: %d", name, errno);
		goto new_clean;
	}

	self->name = strndup(name, strlen(name));
	/*
	 * Have a own copy of the config
	 */
	self->nw_config = strdup(json_object_get_string(nw_obj));

	/*
	 * Alloc mem for array of connection buffers upto max file descriptor val
	 * Note: There might be a small memory wastage if all the array space is
	 * not occupied, but it provides a much faster lookup
	 */
	struct rlimit rlim;
	if (getrlimit(RLIMIT_NOFILE, &rlim) != 0) {
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Could not get the max fd resource"
				 " limit in socket %s: %d", name, errno);
		goto new_clean;
	}
	self->max_fd_rlimit = (int)rlim.rlim_cur;
	self->conn_buf_arr = calloc(self->max_fd_rlimit, sizeof(void *));
	if (!self->conn_buf_arr || (self->max_fd_rlimit <= 0)) {
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Error alloc'g mem for CB array "
				 "with max fd sz: %d in socket %s: %d",
				 self->max_fd_rlimit, name, errno);
		goto new_clean;
	}

	/*
	 * Create the control bus between the socket thr and main thr
	 */
	if (socketpair(AF_UNIX, SOCK_STREAM, 0, self->mctrl_bus) < 0) {
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Error creating main ctrl bus "
				 "for tcp socket %s (%d)", self->name, errno);
		goto new_clean;
	}

	/*
  	 * Start the main thread
	 */	 
	res = pthread_create(&main_thr_id, NULL, tdsr_tcp_main_thr, self);
	if (0 != res) {
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Error creating tdsr tcp main thr"
				 " of socket %s: %d", name, res);
		goto new_clean;
	}

	/*
	 * Wait till the main thread is initialized
	 */
	tdsr_log(TYPE_SYSLOG_ONLY, LOG_INFO, "Waiting for main thread "
			 "to initialize for tcp socket %s", self->name);
	ctrl_cmd_st_t ctrl_msg;
	if (read(self->mctrl_bus[0], &ctrl_msg, sizeof(ctrl_msg)) < 0) {
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Error waiting for the "
		 	"main thread to initialize for tcp socket %s (%d)",
			self->name, errno);
		 goto new_clean;
	}

	if (ctrl_msg.ctrl_cmd != INIT_DONE) {
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Expected init done cmd "
			 "from main thread to initialize for tcp socket %s, but "
			 "received cmd %d", self->name, ctrl_msg.ctrl_cmd);
		goto new_clean;
	}	
		
	tdsr_log(TYPE_SYSLOG_ONLY, LOG_INFO, "Done Waiting for main therad to "
			 "initialize for tcp socket %s", self->name);

	return (self);

new_clean:
	tdsr_tcp_destroy((void **)&self);
	return (NULL);
}

/*
 * Clean up the resources used by the socket and other ordely shutdown 
 * procedures of the socket
 * \param sock : Handle to TDSR TCP sock
 */
int
tdsr_tcp_destroy(void **sock) {

	tdsr_tcp_t *self;

	if (!sock) {
		return (0);
	}

	self = (tdsr_tcp_t *)sock;
	if (!self) {
		return (0);
	}

	close(self->main_epoll_fd);
	close(self->listen_fd);

	if (self->name) {
		free(self->name);
	}

	if (self->nw_config) {
		free (self->nw_config);
	}

	/*
	 * Free all the CB's of the sock along with the CB arr
	 */
	if (self->conn_buf_arr) {
		for (int i = 0; i < self->max_fd_rlimit; i++) {
			if (self->conn_buf_arr[i]) {
				free(self->conn_buf_arr[i]);
			}
		}
		free(self->conn_buf_arr);
	}

	/*
	 * Destroy the CBQ
	 */
	if (self->cbq) {
		if (self->cbq->cb_addr_q) {
			tipc_msock_destroy(&(self->cbq->cb_addr_q));
		}
		if (self->cbq->recv_thr_cb_arr) {
			free(self->cbq->recv_thr_cb_arr);
		}
		free(self->cbq);
	}

	tdsr_tcp_clear_thr_list((void **)&(self->poll_thr_list));

	free (self);
	
	*sock = NULL;

	return (0);
}

/*!
 * Connect to a TDSR TCP socket. This will only be used by the receiving side
 * NOTE: The calling thread should use this connection id along with sock for
 * receiving data
 * \param sock: tdsr tcp socket to connect to
 * \return: connection id(unique to sock) on success
 *			-1 on failure
 */
int
tdsr_tcp_connect(void *sock) {

	tdsr_tcp_t *self = (tdsr_tcp_t *)sock;
	void *cb_addr_q = NULL;
	int con_id;
	pthread_t pth_id;
	int err_code;
	int prod_id;
	recv_thr_cb_info_t *recv_thr_cb_info;

	if (!self) {
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Invalid args to tdsr tcp connect");
		return (-1);
	}

	if (!self->cbq || !self->cbq->cb_addr_q) {
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Could not find CBQ to connect "
				 "at port %d for socket %s", self->port, self->name);
		return (-1);
	}

	pth_id = pthread_self();
	cb_addr_q = self->cbq->cb_addr_q;

	/*
  	 * Receiving threads acts both a consumer and producer   
	 */
	con_id = tipc_msock_connect(cb_addr_q, TIPC_CON_TYPE_CONS, pth_id,
														&err_code);
	if (-1 == con_id) {
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Error connecting to sock to cons"
			 " at port %d for socket %s(%d)", self->port, self->name, err_code);
		return (-1);
	}

	prod_id = tipc_msock_connect(cb_addr_q, TIPC_CON_TYPE_PROD, pth_id,
														&err_code);
	if (-1 == prod_id) {
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Error connecting to sock to prod"
			 " at port %d for socket %s(%d)", self->port, self->name, err_code);
		return (-1);
	}

	recv_thr_cb_info = self->cbq->recv_thr_cb_arr + con_id;
	recv_thr_cb_info->pth_id = pth_id;
	recv_thr_cb_info->con_id = con_id;
	recv_thr_cb_info->prod_id = prod_id;

	return(con_id);
}

/*!
 * Receive message from the socket
 * \param sock: TDSR TCP Socket
 * \param con_id: Socket specific connection id
 * \param buf: Return pointer to the message buffer
 * \param flag: Return flags (TDSR_TCP_MSG_TIMER_EXP ......)
 * \return: Number of bytes received on success
 *			-1 on failure
 */
int
tdsr_tcp_recv(void *sock, int con_id, void **buf, size_t *recv_sz,
			  											uint32_t *flag) { 

	tdsr_tcp_t *self = (tdsr_tcp_t *)sock;
	void *cb_addr_q = NULL;
	void *cb = NULL;
	pthread_t pth_id;
	int err_code;
	recv_thr_cb_info_t *recv_thr_cb_info;

	if (!self || (con_id < 0) || !buf || !recv_sz || !flag)	{
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Invalid args to receive data from"
				 "tdsr tcp socket");
		return (-1);
	}

	if (!self->cbq || !self->cbq->cb_addr_q) {
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Could not find CBQ to receive " 
				 "data at port %d for socket %s", self->port, self->name);
		return (-1);
	}

	cb_addr_q = self->cbq->cb_addr_q;
	recv_thr_cb_info = self->cbq->recv_thr_cb_arr + con_id;

	pth_id = recv_thr_cb_info->pth_id;

	cb = tipc_msock_pop(cb_addr_q, con_id, pth_id, &err_code);
	if (!cb) {
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Error receiving CB from CBQ "
			 "at port %d for socket %s(%d)", self->port, self->name, err_code);
		return (-1);
	}

	/*
	 * Set the cb used by this thread
	 */
	recv_thr_cb_info->cb = cb;

	/*
	 * Get CB head and the current data size
	 */
	err_code = tdsr_cb_get_head(cb, buf, recv_sz);
	if (CB_ERR_OK != err_code) {
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Error getting CB head at port %d"
				 " for socket %s(%d)", self->port, self->name, err_code);
		return (-1);
	}
//----------------TESTING--------------------
//	tdsr_log(TYPE_SYSLOG_ONLY, LOG_INFO, "CB Size: %zd", *recv_sz);
//-----------------------------------------
	return(0);
}

/*!
 * Every tdsr_tcp_recv() should be followed by a tdsr_tcp_recv_done() with
 * the amount of data read from the buffer
 * \param sock: TDSR TCP Socket
 * \param con_id: Socket specific connection id
 * \param read_sz: Amount of data read from the recv buffer
 * \return: 0 on Success
 *			-1 on failure
 */
int
tdsr_tcp_recv_done(void *sock, int con_id, size_t read_sz) {

	tdsr_tcp_t *self = (tdsr_tcp_t *)sock;
	void *cb_addr_q = NULL;
	void *cb = NULL;
	pthread_t pth_id;
	int err_code;
	int prod_id;
	size_t avail_sz;
	recv_thr_cb_info_t *recv_thr_cb_info;

	if (!self || (con_id < 0))	{
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Invalid args to receive done for"
				 "tdsr tcp socket");
		return (-1);
	}

	if (!self->cbq || !self->cbq->cb_addr_q) {
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Could not find CBQ to receive " 
				 "done at port %d for socket %s", self->port, self->name);
		return (-1);
	}
	cb_addr_q = self->cbq->cb_addr_q;

	/*
	 * Get the CB and other recv thr info used by this thread
	 */
	recv_thr_cb_info = self->cbq->recv_thr_cb_arr + con_id;
	cb = recv_thr_cb_info->cb;
	recv_thr_cb_info->cb = NULL;
	pth_id = recv_thr_cb_info->pth_id;
	prod_id = recv_thr_cb_info->prod_id;

	__sync_fetch_and_add(&(self->pop_tot_sz), read_sz);
	/*
	 * Set CB head
	 */
	err_code = tdsr_cb_set_head(cb, read_sz);
	if (CB_ERR_OK != err_code) {
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Error setting CB head at port %d",
				 " for socket %s(%d)", self->port, self->name, err_code);
		return (-1);
	}
	
	/*
	 * If CB is empty, do not push it back to CBQ
	 */
	err_code = tdsr_cb_get_avail_sz(cb, &avail_sz);
	if (CB_ERR_OK != err_code) {
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Error getting CB avail sz at port",
				 " %d for socket %s(%d)", self->port, self->name, err_code);
		return (-1);
	}

	if (avail_sz == 0) {
		/*
		 * If connection closed flag is set for the CB then destroy the CB,
		 * else just set the CB empty flag
		 */
		if (tdsr_cb_get_is_con_closed(cb) == true) {
			tdsr_cb_destroy(&cb);
		} else {
			tdsr_cb_set_is_empty(cb, true);
			tdsr_log(TYPE_SYSLOG_ONLY, LOG_DEBUG, "CB is empty after recv. Not "
				 "pushing into CBQ");
		}
	} else {
		/*
		 * Put the CB back to CBQ
		 */
		err_code = tipc_msock_push(cb_addr_q, prod_id, pth_id, cb);
		if (CB_ERR_OK != err_code) {
			tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Error pushing CB into CBQ at ",
				 "port %d for socket %s(%d)", self->port, self->name, err_code);
			return (-1);
		}
	}
	return (0);
}

/*!
 * Print TDSR TCP sock stats
 * \param sock: Pointer to the socket
 */
void
tdsr_tcp_print_stat(void *sock) {

	tdsr_tcp_t *self = (tdsr_tcp_t *)sock;

	if (!sock) {
		tdsr_log(TYPE_SYSLOG_ONLY, LOG_ERR, "Invalid args to print tcp sock "
				 "stats");
		return;
	}
	tdsr_log(TYPE_SYSLOG_ONLY, LOG_INFO, "Socket %s [Push %ld : Pop %ld]",
			 self->name, self->push_tot_sz, self->pop_tot_sz);
	return;
}
