/*
 * Should be compiled with:
 *
 * gcc -Wall -O2 -g -D_GNU_SOURCE -include ../config-host.h -shared -rdynamic -fPIC -o skeleton_external.o skeleton_external.c
 * (also requires -D_GNU_SOURCE -DCONFIG_STRSEP on Linux)
 * 
 * Custom fio plugin to test the performance of transfering data through the network
 * and then issuing local read/writes using the buffer received.
 *
 */
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <assert.h>
#include <netdb.h>

#include "../fio.h"
#include "../optgroup.h"

#include <rdma/rdma_cma.h>

#define GOLDEN_RATIO_PRIME 0x9e37fffffffc0001UL

#define FIO_RDMA_MAX_IO_DEPTH    512

enum rdma_rw_mode {
	FIO_RDMA_RW_READ,
	FIO_RDMA_RW_WRITE,
};

/*
 * The io engine can define its own options within the io engine source.
 * The option member must not be at offset 0, due to the way fio parses
 * the given option. Just add a padding pointer unless the io engine has
 * something usable.
 */
struct rdma_rw_options {
	struct thread_data *td;
	unsigned int port;
	enum rdma_rw_mode verb;
	char *bindname;
};

static struct fio_option options[] = {
	{
		.name	= "port",
		.lname	= "rdma_rw engine port",
		.type	= FIO_OPT_INT,
		.off1	= offsetof(struct rdma_rw_options, port),
		.help	= "Port to use for RDMA connections",
		.minval	= 1,
		.maxval	= 65535,
		.category = FIO_OPT_C_ENGINE, /* always use this */
		.group	= FIO_OPT_G_RDMA_RW, /* this can be different */
	},
	{
		.name	= "verb",
		.lname	= "RDMA engine verb",
		.alias	= "proto",
		.type	= FIO_OPT_STR,
		.off1	= offsetof(struct rdma_rw_options, verb),
		.help	= "RDMA engine verb",
		.def	= "write",
		.posval = {
			  { .ival = "write",
			    .oval = FIO_RDMA_RW_WRITE,
			    .help = "Memory Write",
			  },
			  { .ival = "read",
			    .oval = FIO_RDMA_RW_READ,
			    .help = "Memory Read",
			  },
		},
		.category = FIO_OPT_C_ENGINE,
		.group	= FIO_OPT_G_RDMA_RW,
	},
	{
		.name	= NULL,
	},
};

struct remote_u {
	uint64_t buf;
	uint32_t rkey;
	uint32_t size;
};

struct rdma_rw_info_blk {
	uint32_t mode;		/* channel semantic or memory semantic */
	uint32_t nr;		/* client: io depth
						   server: number of records for memory semantic
						*/
	uint32_t max_bs;        /* maximum block size */
	struct remote_u rmt_us[FIO_RDMA_MAX_IO_DEPTH];
};

struct rdma_io_u_data {
	uint64_t wr_id;
	struct ibv_send_wr sq_wr;
	struct ibv_recv_wr rq_wr;
	struct ibv_sge rdma_sgl;
};

struct rdma_rw_data {
	int is_client;
	enum rdma_rw_mode rdma_protocol;
	char host[64];
	struct sockaddr_in addr;

	struct ibv_recv_wr rq_wr;
	struct ibv_sge recv_sgl;
	struct rdma_rw_info_blk recv_buf;
	struct ibv_mr *recv_mr;

	struct ibv_send_wr sq_wr;
	struct ibv_sge send_sgl;
	struct rdma_rw_info_blk send_buf;
	struct ibv_mr *send_mr;

	struct ibv_comp_channel *channel;
	struct ibv_cq *cq;
	struct ibv_pd *pd;
	struct ibv_qp *qp;

	pthread_t cmthread;
	struct rdma_event_channel *cm_channel;
	struct rdma_cm_id *cm_id;
	struct rdma_cm_id *child_cm_id;

	int cq_event_num;

	struct remote_u *rmt_us;
	int rmt_nr;
	struct io_u **io_us_queued;
	int io_u_queued_nr;
	struct io_u **io_us_flight;
	int io_u_flight_nr;
	struct io_u **io_us_completed;
	int io_u_completed_nr;

	struct frand_state rand_state;
};

/*
 * The ->event() hook is called to match an event number with an io_u.
 * After the core has called ->getevents() and it has returned eg 3,
 * the ->event() hook must return the 3 events that have completed for
 * subsequent calls to ->event() with [0-2]. Required.
 */
static struct io_u *fio_rdma_rw_event(struct thread_data *td, int event)
{
	return NULL;
}

/*
 * The ->getevents() hook is used to reap completion events from an async
 * io engine. It returns the number of completed events since the last call,
 * which may then be retrieved by calling the ->event() hook with the event
 * numbers. Required.
 */
static int fio_rdma_rw_getevents(struct thread_data *td, unsigned int min,
				  unsigned int max, const struct timespec *t)
{
	return 0;
}

/*
 * The ->queue() hook is responsible for initiating io on the io_u
 * being passed in. If the io engine is a synchronous one, io may complete
 * before ->queue() returns. Required.
 *
 * The io engine must transfer in the direction noted by io_u->ddir
 * to the buffer pointed to by io_u->xfer_buf for as many bytes as
 * io_u->xfer_buflen. Residual data count may be set in io_u->resid
 * for a short read/write.
 */
static enum fio_q_status fio_rdma_rw_queue(struct thread_data *td,
					    struct io_u *io_u)
{
	/*
	 * Double sanity check to catch errant write on a readonly setup
	 */
	fio_ro_check(td, io_u);

	/*
	 * Could return FIO_Q_QUEUED for a queued request,
	 * FIO_Q_COMPLETED for a completed request, and FIO_Q_BUSY
	 * if we could queue no more at this point (you'd have to
	 * define ->commit() to handle that.
	 */
	return FIO_Q_COMPLETED;
}


static int fio_rdma_rw_commit(struct thread_data *td) {
	return 0;
}

/*
 * The ->prep() function is called for each io_u prior to being submitted
 * with ->queue(). This hook allows the io engine to perform any
 * preparatory actions on the io_u, before being submitted. Not required.
 */
static int fio_rdma_rw_prep(struct thread_data *td, struct io_u *io_u)
{
	return 0;
}

static int fio_rdma_rw_setup(struct thread_data *td) {
	struct rdma_rw_data *rd;

	if (!td->files_index) {
		add_file(td, td->o.filename ?: "rdma_rw", 0, 0);
		td->o.nr_files = td->o.nr_files ?: 1;
		td->o.open_files++;
	}

	if (!td->io_ops_data) {
		rd = malloc(sizeof(*rd));

		memset(rd, 0, sizeof(*rd));
		init_rand_seed(&rd->rand_state, (unsigned int) GOLDEN_RATIO_PRIME, 0);
		td->io_ops_data = rd;
	}

	return 0;
}

static int get_next_channel_event(struct thread_data *td,
				  struct rdma_event_channel *channel,
				  enum rdma_cm_event_type wait_event)
{
	struct rdma_rw_data *rd = td->io_ops_data;
	struct rdma_cm_event *event;
	int ret;

	ret = rdma_get_cm_event(channel, &event);
	if (ret) {
		log_err("fio: rdma_get_cm_event: %d\n", ret);
		return 1;
	}

	if (event->event != wait_event) {
		log_err("fio: event is %s instead of %s\n",
			rdma_event_str(event->event),
			rdma_event_str(wait_event));
		return 1;
	}

	switch (event->event) {
	case RDMA_CM_EVENT_CONNECT_REQUEST:
		rd->child_cm_id = event->id;
		break;
	default:
		break;
	}

	rdma_ack_cm_event(event);

	return 0;
}

static int aton(struct thread_data *td, const char *host,
		     struct sockaddr_in *addr)
{
	if (inet_aton(host, &addr->sin_addr) != 1) {
		struct hostent *hent;

		hent = gethostbyname(host);
		if (!hent) {
			td_verror(td, errno, "gethostbyname");
			return 1;
		}

		memcpy(&addr->sin_addr, hent->h_addr, 4);
	}
	return 0;
}

static int fio_rdma_rw_setup_qp(struct thread_data *td)
{
	struct rdma_rw_data *rd = td->io_ops_data;
	struct ibv_qp_init_attr init_attr;
	int qp_depth = td->o.iodepth * 2;	/* 2 times of io depth */

	if (rd->is_client == 0)
		rd->pd = ibv_alloc_pd(rd->child_cm_id->verbs);
	else
		rd->pd = ibv_alloc_pd(rd->cm_id->verbs);

	if (rd->pd == NULL) {
		log_err("fio: ibv_alloc_pd fail: %m\n");
		return 1;
	}

	if (rd->is_client == 0)
		rd->channel = ibv_create_comp_channel(rd->child_cm_id->verbs);
	else
		rd->channel = ibv_create_comp_channel(rd->cm_id->verbs);
	if (rd->channel == NULL) {
		log_err("fio: ibv_create_comp_channel fail: %m\n");
		goto err1;
	}

	if (qp_depth < 16)
		qp_depth = 16;

	if (rd->is_client == 0)
		rd->cq = ibv_create_cq(rd->child_cm_id->verbs,
				       qp_depth, rd, rd->channel, 0);
	else
		rd->cq = ibv_create_cq(rd->cm_id->verbs,
				       qp_depth, rd, rd->channel, 0);
	if (rd->cq == NULL) {
		log_err("fio: ibv_create_cq failed: %m\n");
		goto err2;
	}

	if (ibv_req_notify_cq(rd->cq, 0) != 0) {
		log_err("fio: ibv_req_notify_cq failed: %m\n");
		goto err3;
	}

	/* create queue pair */
	memset(&init_attr, 0, sizeof(init_attr));
	init_attr.cap.max_send_wr = qp_depth;
	init_attr.cap.max_recv_wr = qp_depth;
	init_attr.cap.max_recv_sge = 1;
	init_attr.cap.max_send_sge = 1;
	init_attr.qp_type = IBV_QPT_RC;
	init_attr.send_cq = rd->cq;
	init_attr.recv_cq = rd->cq;

	if (rd->is_client == 0) {
		if (rdma_create_qp(rd->child_cm_id, rd->pd, &init_attr) != 0) {
			log_err("fio: rdma_create_qp failed: %m\n");
			goto err3;
		}
		rd->qp = rd->child_cm_id->qp;
	} else {
		if (rdma_create_qp(rd->cm_id, rd->pd, &init_attr) != 0) {
			log_err("fio: rdma_create_qp failed: %m\n");
			goto err3;
		}
		rd->qp = rd->cm_id->qp;
	}

	return 0;

err3:
	ibv_destroy_cq(rd->cq);
err2:
	ibv_destroy_comp_channel(rd->channel);
err1:
	ibv_dealloc_pd(rd->pd);

	return 1;
}

static int fio_rdma_rw_setup_control_msg_buffers(struct thread_data *td)
{
	struct rdma_rw_data *rd = td->io_ops_data;

	rd->recv_mr = ibv_reg_mr(rd->pd, &rd->recv_buf, sizeof(rd->recv_buf),
				 IBV_ACCESS_LOCAL_WRITE);
	if (rd->recv_mr == NULL) {
		log_err("fio: recv_buf reg_mr failed: %m\n");
		return 1;
	}

	rd->send_mr = ibv_reg_mr(rd->pd, &rd->send_buf, sizeof(rd->send_buf),
				 0);
	if (rd->send_mr == NULL) {
		log_err("fio: send_buf reg_mr failed: %m\n");
		ibv_dereg_mr(rd->recv_mr);
		return 1;
	}

	/* setup work request */
	/* recv wq */
	rd->recv_sgl.addr = (uint64_t) (unsigned long)&rd->recv_buf;
	rd->recv_sgl.length = sizeof(rd->recv_buf);
	rd->recv_sgl.lkey = rd->recv_mr->lkey;
	rd->rq_wr.sg_list = &rd->recv_sgl;
	rd->rq_wr.num_sge = 1;
	rd->rq_wr.wr_id = FIO_RDMA_MAX_IO_DEPTH;

	/* send wq */
	rd->send_sgl.addr = (uint64_t) (unsigned long)&rd->send_buf;
	rd->send_sgl.length = sizeof(rd->send_buf);
	rd->send_sgl.lkey = rd->send_mr->lkey;

	rd->sq_wr.opcode = IBV_WR_SEND;
	rd->sq_wr.send_flags = IBV_SEND_SIGNALED;
	rd->sq_wr.sg_list = &rd->send_sgl;
	rd->sq_wr.num_sge = 1;
	rd->sq_wr.wr_id = FIO_RDMA_MAX_IO_DEPTH;

	return 0;
}

static int fio_rdma_rw_setup_connect(struct thread_data *td, const char *host,
				    unsigned short port)
{
	struct rdma_rw_data *rd = td->io_ops_data;
	struct rdma_rw_options *o = td->eo;
	struct sockaddr_storage addrb;
	struct ibv_recv_wr *bad_wr;
	int err;

	rd->addr.sin_family = AF_INET;
	rd->addr.sin_port = htons(port);

	err = aton(td, host, &rd->addr);
	if (err)
		return err;

	/* resolve route */
	if (o->bindname && strlen(o->bindname)) {
		addrb.ss_family = AF_INET;
		err = aton(td, o->bindname, (struct sockaddr_in *)&addrb);
		if (err)
			return err;
		err = rdma_resolve_addr(rd->cm_id, (struct sockaddr *)&addrb,
					(struct sockaddr *)&rd->addr, 2000);

	} else {
		err = rdma_resolve_addr(rd->cm_id, NULL,
					(struct sockaddr *)&rd->addr, 2000);
	}

	if (err != 0) {
		log_err("fio: rdma_resolve_addr: %d\n", err);
		return 1;
	}

	err = get_next_channel_event(td, rd->cm_channel, RDMA_CM_EVENT_ADDR_RESOLVED);
	if (err != 0) {
		log_err("fio: get_next_channel_event: %d\n", err);
		return 1;
	}

	/* resolve route */
	err = rdma_resolve_route(rd->cm_id, 2000);
	if (err != 0) {
		log_err("fio: rdma_resolve_route: %d\n", err);
		return 1;
	}

	err = get_next_channel_event(td, rd->cm_channel, RDMA_CM_EVENT_ROUTE_RESOLVED);
	if (err != 0) {
		log_err("fio: get_next_channel_event: %d\n", err);
		return 1;
	}

	/* create qp and buffer */
	if (fio_rdma_rw_setup_qp(td) != 0)
		return 1;

	if (fio_rdma_rw_setup_control_msg_buffers(td) != 0)
		return 1;

	/* post recv buf */
	err = ibv_post_recv(rd->qp, &rd->rq_wr, &bad_wr);
	if (err != 0) {
		log_err("fio: ibv_post_recv fail: %d\n", err);
		return 1;
	}

	return 0;
}

static int fio_rdma_rw_setup_listen(struct thread_data *td, short port)
{
	struct rdma_rw_data *rd = td->io_ops_data;
	struct rdma_rw_options *o = td->eo;
	struct ibv_recv_wr *bad_wr;
	int state = td->runstate;

	td_set_runstate(td, TD_SETTING_UP);

	rd->addr.sin_family = AF_INET;
	rd->addr.sin_port = htons(port);

	// bindname can be the IPv4/IPv6 address
	if (!o->bindname || !strlen(o->bindname))
		rd->addr.sin_addr.s_addr = htonl(INADDR_ANY);
	else
		rd->addr.sin_addr.s_addr = htonl(*o->bindname);

	/* rdma_listen */
	if (rdma_bind_addr(rd->cm_id, (struct sockaddr *)&rd->addr) != 0) {
		log_err("fio: rdma_bind_addr fail: %m\n");
		return 1;
	}

	if (rdma_listen(rd->cm_id, 3) != 0) {
		log_err("fio: rdma_listen fail: %m\n");
		return 1;
	}

	log_info("fio: waiting for connection\n");

	/* wait for CONNECT_REQUEST */
	if (get_next_channel_event
	    (td, rd->cm_channel, RDMA_CM_EVENT_CONNECT_REQUEST) != 0) {
		log_err("fio: wait for RDMA_CM_EVENT_CONNECT_REQUEST\n");
		return 1;
	}

	if (fio_rdma_rw_setup_qp(td) != 0)
		return 1;

	if (fio_rdma_rw_setup_control_msg_buffers(td) != 0)
		return 1;

	/* post recv buf */
	if (ibv_post_recv(rd->qp, &rd->rq_wr, &bad_wr) != 0) {
		log_err("fio: ibv_post_recv fail: %m\n");
		return 1;
	}

	td_set_runstate(td, state);
	return 0;
}

/*
 * The init function is called once per thread/process, and should set up
 * any structures that this io engine requires to keep track of io. Not
 * required.
 */
static int fio_rdma_rw_init(struct thread_data *td)
{
	struct rdma_rw_data *rd = td->io_ops_data;
	struct rdma_rw_options *o = td->eo;
	int ret;

	if (td_rw(td)) {
		log_err("fio: rdma connections must be read OR write\n");
		return 1;
	}

	if (td_random(td)) {
		log_err("fio: RDMA network IO can't be random\n");
		return 1;
	}

	if (!o->port) {
		log_err("fio: no port has been specified which is required "
			"for the rdma engine\n");
		return 1;
	}

	rd->rdma_protocol = o->verb;
	rd->cq_event_num = 0;

	rd->cm_channel = rdma_create_event_channel();
	if (!rd->cm_channel) {
		log_err("fio: rdma_create_event_channel fail: %m\n");
		return 1;
	}

	ret = rdma_create_id(rd->cm_channel, &rd->cm_id, rd, RDMA_PS_TCP);
	if (ret) {
		log_err("fio: rdma_create_id fail: %m\n");
		return 1;
	}

	if ((rd->rdma_protocol == FIO_RDMA_RW_WRITE) ||
	    (rd->rdma_protocol == FIO_RDMA_RW_WRITE)) {
		rd->rmt_us =
			malloc(FIO_RDMA_MAX_IO_DEPTH * sizeof(struct remote_u));
		memset(rd->rmt_us, 0,
			FIO_RDMA_MAX_IO_DEPTH * sizeof(struct remote_u));
		rd->rmt_nr = 0;
	}

	rd->io_us_queued = malloc(td->o.iodepth * sizeof(struct io_u *));
	memset(rd->io_us_queued, 0, td->o.iodepth * sizeof(struct io_u *));
	rd->io_u_queued_nr = 0;

	rd->io_us_flight = malloc(td->o.iodepth * sizeof(struct io_u *));
	memset(rd->io_us_flight, 0, td->o.iodepth * sizeof(struct io_u *));
	rd->io_u_flight_nr = 0;

	rd->io_us_completed = malloc(td->o.iodepth * sizeof(struct io_u *));
	memset(rd->io_us_completed, 0, td->o.iodepth * sizeof(struct io_u *));
	rd->io_u_completed_nr = 0;

	if (td_read(td)) {	/* READ as the server */
		rd->is_client = 0;
		td->flags |= TD_F_NO_PROGRESS;
		/* server rd->rdma_buf_len will be setup after got request */
		ret = fio_rdma_rw_setup_listen(td, o->port);
	} else {		/* WRITE as the client */
		rd->is_client = 1;
		ret = fio_rdma_rw_setup_connect(td, td->o.filename, o->port);
	}
	return ret;
}


static int fio_rdma_rw_post_init(struct thread_data *td)
{
	unsigned int max_bs;
	int i;
	struct rdma_rw_data *rd = td->io_ops_data;

	max_bs = max(td->o.max_bs[DDIR_READ], td->o.max_bs[DDIR_WRITE]);
	rd->send_buf.max_bs = htonl(max_bs);

	/* register each io_u in the free list */
	for (i = 0; i < td->io_u_freelist.nr; i++) {
		struct io_u *io_u = td->io_u_freelist.io_us[i];

		io_u->engine_data = malloc(sizeof(struct rdma_io_u_data));
		memset(io_u->engine_data, 0, sizeof(struct rdma_io_u_data));
		((struct rdma_io_u_data *)io_u->engine_data)->wr_id = i;

		io_u->mr = ibv_reg_mr(rd->pd, io_u->buf, max_bs,
				      IBV_ACCESS_LOCAL_WRITE |
				      IBV_ACCESS_REMOTE_READ |
				      IBV_ACCESS_REMOTE_WRITE);
		if (io_u->mr == NULL) {
			log_err("fio: ibv_reg_mr io_u failed: %m\n");
			return 1;
		}

		rd->send_buf.rmt_us[i].buf =
		    cpu_to_be64((uint64_t) (unsigned long)io_u->buf);
		rd->send_buf.rmt_us[i].rkey = htonl(io_u->mr->rkey);
		rd->send_buf.rmt_us[i].size = htonl(max_bs);

	}
	rd->send_buf.nr = htonl(i);

	return 0;
}

/*
 * This is paired with the ->init() function and is called when a thread is
 * done doing io. Should tear down anything setup by the ->init() function.
 * Not required.
 */
static void fio_rdma_rw_cleanup(struct thread_data *td)
{
	struct rdmaio_data *rd = td->io_ops_data;

	if (rd)
		free(rd);
}

/*
 * Hook for opening the given file. Unless the engine has special
 * needs, it usually just provides generic_open_file() as the handler.
 */
static int fio_rdma_rw_open(struct thread_data *td, struct fio_file *f)
{
	return generic_open_file(td, f);
}

/*
 * Hook for closing a file. See fio_skeleton_open().
 */
static int fio_rdma_rw_close(struct thread_data *td, struct fio_file *f)
{
	return generic_close_file(td, f);
}


/*
 * Note that the structure is exported, so that fio can get it via
 * dlsym(..., "ioengine"); for (and only for) external engines.
 */
struct ioengine_ops ioengine = {
	.name		= "rdma_rw",
	.version	= FIO_IOOPS_VERSION,
	.setup		= fio_rdma_rw_setup,
	.init		= fio_rdma_rw_init,
	.prep		= fio_rdma_rw_prep,
	.post_init	= fio_rdma_rw_post_init,
	.queue		= fio_rdma_rw_queue,
	.commit		= fio_rdma_rw_commit,
	.getevents	= fio_rdma_rw_getevents,
	.event		= fio_rdma_rw_event,
	.cleanup	= fio_rdma_rw_cleanup,
	.open_file	= fio_rdma_rw_open,
	.close_file	= fio_rdma_rw_close,
	.options	= options,
	.option_struct_size	= sizeof(struct rdma_rw_options),
};
