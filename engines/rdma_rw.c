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
#include <sys/stat.h>

#include "../fio.h"
#include "../optgroup.h"

#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>

#define GOLDEN_RATIO_PRIME 0x9e37fffffffc0001UL

#define FIO_RDMA_MAX_IO_DEPTH    512


enum rdma_rw_mode {
	FIO_RDMA_RW_READ,
	FIO_RDMA_RW_WRITE,
};

enum rdma_rw_control_opcode {
	INVALID_ID,
	FLUSH_WR_ID,
	FLUSH_ACK_WR_ID,
	END_WR_ID,
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
	unsigned long flushsize;
};

static int str_hostname_cb(void *data, const char *input)
{
	struct rdma_rw_options *o = data;

	if (o->td->o.filename)
		free(o->td->o.filename);
	o->td->o.filename = strdup(input);
	return 0;
}

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
		.name	= "hostname",
		.lname	= "rdma engine hostname",
		.type	= FIO_OPT_STR_STORE,
		.cb	= str_hostname_cb,
		.help	= "Hostname for RDMA IO engine",
		.category = FIO_OPT_C_ENGINE,
		.group	= FIO_OPT_G_RDMA_RW,
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
		.name		= "flushsize",
		.lname		= "RDMA-rw flush size",
		.type		= FIO_OPT_STR_VAL,
		.off1		= offsetof(struct rdma_rw_options, flushsize),
		.help		= "flush size for RDMA-rw",
		.category	= FIO_OPT_C_ENGINE,
		.group		= FIO_OPT_G_RDMA_RW,
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

struct rdma_rw_flush_info_blk {
	enum rdma_rw_control_opcode opcode;
	unsigned long flush_turn;
};

struct rdma_rw_info_blk {
	uint32_t mode;		/* channel semantic or memory semantic */
	uint32_t nr;		/* client: io depth
						   server: number of records for memory semantic
						*/
	uint32_t max_bs;        /* maximum block size */
	struct remote_u rmt_us[FIO_RDMA_MAX_IO_DEPTH];
};

struct rdma_rw_u_data {
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

	// rdma-rw specific
	void *flush_buf;
	unsigned long flush_turn;
	int backend_log_fd;
	size_t backend_log_size;
	size_t cached_len;
	size_t flushsize;
	struct rdma_cm_id *flush_listen_cm_id;
	struct rdma_cm_id *flush_cm_id;
	struct rdma_event_channel *flush_cm_channel;
	struct ibv_cq *flush_cq;
	struct ibv_comp_channel *flush_comp_channel;
	struct ibv_qp *flush_qp;
	struct ibv_pd *flush_pd;
	struct ibv_send_wr flush_sq_wr;
	struct ibv_recv_wr flush_rq_wr;
	struct ibv_mr *flush_send_mr;
	struct ibv_mr *flush_recv_mr;
	struct rdma_rw_flush_info_blk flush_recv_buf;
	struct rdma_rw_flush_info_blk flush_send_buf;
	struct ibv_sge flush_recv_sgl;
	struct ibv_sge flush_send_sgl;
	int flush_cq_event_num;
};


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
				  enum rdma_cm_event_type wait_event, int flush_channel)
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
		if (!flush_channel)
			rd->child_cm_id = event->id;
		else
			rd->flush_cm_id = event->id;
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

static int fio_rdma_rw_setup_flush_qp(struct thread_data *td)
{
	struct rdma_rw_data *rd = td->io_ops_data;
	struct ibv_qp_init_attr init_attr;
	int qp_depth = 2;

	rd->flush_pd = ibv_alloc_pd(rd->flush_cm_id->verbs);

	if (rd->flush_pd == NULL) {
		log_err("fio: ibv_alloc_pd fail: %m\n");
		return 1;
	}

	rd->flush_comp_channel = ibv_create_comp_channel(rd->flush_cm_id->verbs);
	if (rd->flush_comp_channel == NULL) {
		log_err("fio: ibv_create_comp_channel fail: %m\n");
		goto err1;
	}

	rd->flush_cq = ibv_create_cq(rd->flush_cm_id->verbs,
				       qp_depth, rd, rd->flush_comp_channel, 0);
	if (rd->flush_cq == NULL) {
		log_err("fio: ibv_create_cq failed: %m\n");
		goto err2;
	}

	if (ibv_req_notify_cq(rd->flush_cq, 0) != 0) {
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
	init_attr.send_cq = rd->flush_cq;
	init_attr.recv_cq = rd->flush_cq;

	if (rdma_create_qp(rd->flush_cm_id, rd->flush_pd, &init_attr) != 0) {
		log_err("fio: rdma_create_qp failed: %m\n");
		goto err3;
	}
	rd->flush_qp = rd->flush_cm_id->qp;

	return 0;

err3:
	ibv_destroy_cq(rd->flush_cq);
err2:
	ibv_destroy_comp_channel(rd->flush_comp_channel);
err1:
	ibv_dealloc_pd(rd->flush_pd);


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

	err = get_next_channel_event(td, rd->cm_channel, RDMA_CM_EVENT_ADDR_RESOLVED, 0);
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

	err = get_next_channel_event(td, rd->cm_channel, RDMA_CM_EVENT_ROUTE_RESOLVED, 0);
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
	    (td, rd->cm_channel, RDMA_CM_EVENT_CONNECT_REQUEST, 0) != 0) {
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

static int fio_rdma_rw_setup_flush_control_buffers(struct thread_data *td) {
	struct rdma_rw_data *rd = td->io_ops_data;

	rd->flush_recv_mr = ibv_reg_mr(rd->flush_pd, &rd->flush_recv_buf,
			sizeof(rd->flush_recv_buf), IBV_ACCESS_LOCAL_WRITE);
	if (rd->flush_recv_mr == NULL) {
		log_err("fio: flush_recv_mr reg_mr fail: %m\n");
		return 1;
	}

	rd->flush_send_mr = ibv_reg_mr(rd->flush_pd, &rd->flush_send_buf,
			sizeof(rd->flush_send_buf), 0);
	if (rd->flush_send_mr == NULL) {
		log_err("fio: flush_send_mr reg_mr fail: %m\n");
		return 1;
	}

	rd->flush_recv_sgl.addr = (uint64_t) (unsigned long)&rd->flush_recv_buf;
	rd->flush_recv_sgl.length = sizeof(rd->flush_recv_buf);
	rd->flush_recv_sgl.lkey = rd->flush_recv_mr->lkey;
	rd->flush_rq_wr.sg_list = &rd->flush_recv_sgl;
	rd->flush_rq_wr.num_sge = 1;


	rd->flush_send_sgl.addr = (uint64_t) (unsigned long)&rd->flush_send_buf;
	rd->flush_send_sgl.length = sizeof(rd->flush_send_buf);
	rd->flush_send_sgl.lkey = rd->flush_send_mr->lkey;

	rd->flush_sq_wr.opcode = IBV_WR_SEND;
	rd->flush_sq_wr.send_flags = IBV_SEND_SIGNALED;
	rd->flush_sq_wr.sg_list = &rd->flush_send_sgl;
	rd->flush_sq_wr.num_sge = 1;
	if (rd->is_client) {
		rd->flush_sq_wr.wr_id = FLUSH_WR_ID;
		rd->flush_send_buf.opcode = FLUSH_WR_ID;
	} else {
		rd->flush_sq_wr.wr_id = FLUSH_ACK_WR_ID;
		rd->flush_send_buf.opcode = FLUSH_ACK_WR_ID;
	}

	return 0;
}

static int fio_rdma_rw_server_setup_flush(struct thread_data *td) {
	struct rdma_rw_data *rd = td->io_ops_data;
	struct rdma_rw_options *o = td->eo;
	int ret;
	int state = td->runstate;
	struct sockaddr_in addr;

	td_set_runstate(td, TD_SETTING_UP);

	rd->flush_cm_channel = rdma_create_event_channel();
	if (!rd->flush_cm_channel) {
		log_err("fio: rdma_create_event_channel fail: %m\n");
		return 1;
	}

	ret = rdma_create_id(rd->flush_cm_channel, &rd->flush_listen_cm_id, rd, RDMA_PS_TCP);
	if (ret) {
		log_err("fio: rdma_create_id fail: %m\n");
		return 1;
	}

	addr.sin_family = AF_INET;
	addr.sin_port = htons(o->port + 1);
	addr.sin_addr.s_addr = htonl(INADDR_ANY);

	if (rdma_bind_addr(rd->flush_listen_cm_id, (struct sockaddr *)&addr) != 0) {
		log_err("fio: rdma_bind_addr fail: %m\n");
		return 1;
	}

	if (rdma_listen(rd->flush_listen_cm_id, 3) != 0) {
		log_err("fio: rdma_listen fail: %m\n");
		return 1;
	}

	if (get_next_channel_event
	    (td, rd->flush_cm_channel, RDMA_CM_EVENT_CONNECT_REQUEST, 1) != 0) {
		log_err("fio: wait for RDMA_CM_EVENT_CONNECT_REQUEST\n");
		return 1;
	}

	if (fio_rdma_rw_setup_flush_qp(td) != 0)
		return 1;

	if (fio_rdma_rw_setup_flush_control_buffers(td) != 0)
		return 1;

	td_set_runstate(td, state);
	return 0;
}

static int fio_rdma_rw_client_setup_flush(struct thread_data *td) {
	struct rdma_rw_data *rd = td->io_ops_data;
	struct rdma_rw_options *o = td->eo;
	struct sockaddr_in addr;
	int ret = 0;

	rd->flush_cm_channel = rdma_create_event_channel();
	if (!rd->flush_cm_channel) {
		log_err("fio: rdma_create_event_channel fail: %m\n");
		return 1;
	}

	ret = rdma_create_id(rd->flush_cm_channel, &rd->flush_cm_id, rd, RDMA_PS_TCP);
	if (ret) {
		log_err("fio: rdma_create_id fail: %m\n");
		return 1;
	}

	memset(&addr, 0, sizeof(struct sockaddr_in));
	addr.sin_family = AF_INET;
	addr.sin_port = htons(o->port + 1);

	ret = aton(td, td->o.filename, &addr);
	if (ret)
		return ret;

	ret = rdma_resolve_addr(rd->flush_cm_id, NULL,
		(struct sockaddr *)&addr, 2000);

	if (ret != 0) {
		log_err("fio: rdma_resolve_addr: %d\n", ret);
		return 1;
	}

	ret = get_next_channel_event(td, rd->flush_cm_channel, RDMA_CM_EVENT_ADDR_RESOLVED, 1);
	if (ret != 0) {
		log_err("fio: get_next_channel_event: %d\n", ret);
		return 1;
	}

	ret = rdma_resolve_route(rd->flush_cm_id, 2000);
	if (ret != 0) {
		log_err("fio: rdma_resolve_route: %d\n", ret);
		return 1;
	}

	ret = get_next_channel_event(td, rd->flush_cm_channel, RDMA_CM_EVENT_ROUTE_RESOLVED, 1);
	if (ret != 0) {
		log_err("fio: get_next_channel_event: %d\n", ret);
		return 1;
	}

	if (fio_rdma_rw_setup_flush_qp(td) != 0)
		return 1;

	if (fio_rdma_rw_setup_flush_control_buffers(td) != 0)
		return 1;

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
	rd->flushsize = o->flushsize;

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
	    (rd->rdma_protocol == FIO_RDMA_RW_READ)) {
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

	// rmda-rw specific
	rd->flushsize = o->flushsize;
	if (!rd->is_client) {
		rd->flush_buf = fio_memalign(page_size, o->flushsize, 0);
	} else {
		fio_rdma_rw_client_setup_flush(td);
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

		io_u->engine_data = malloc(sizeof(struct rdma_rw_u_data));
		memset(io_u->engine_data, 0, sizeof(struct rdma_rw_u_data));
		((struct rdma_rw_u_data *)io_u->engine_data)->wr_id = i;

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

static int client_recv(struct thread_data *td, struct ibv_wc *wc)
{
	struct rdma_rw_data *rd = td->io_ops_data;
	unsigned int max_bs;

	if (wc->byte_len != sizeof(rd->recv_buf)) {
		log_err("Received bogus data, size %d\n", wc->byte_len);
		return 1;
	}

	max_bs = max(td->o.max_bs[DDIR_READ], td->o.max_bs[DDIR_WRITE]);
	if (max_bs > ntohl(rd->recv_buf.max_bs)) {
		log_err("fio: Server's block size (%d) must be greater than or "
			"equal to the client's block size (%d)!\n",
			ntohl(rd->recv_buf.max_bs), max_bs);
		return 1;
	}

	/* store mr info for MEMORY semantic */
	if ((rd->rdma_protocol == FIO_RDMA_RW_WRITE) ||
	    (rd->rdma_protocol == FIO_RDMA_RW_READ)) {
		/* struct flist_head *entry; */
		int i = 0;

		rd->rmt_nr = ntohl(rd->recv_buf.nr);

		for (i = 0; i < rd->rmt_nr; i++) {
			rd->rmt_us[i].buf = __be64_to_cpu(
						rd->recv_buf.rmt_us[i].buf);
			rd->rmt_us[i].rkey = ntohl(rd->recv_buf.rmt_us[i].rkey);
			rd->rmt_us[i].size = ntohl(rd->recv_buf.rmt_us[i].size);

			dprint(FD_IO,
			       "fio: Received rkey %x addr %" PRIx64
			       " len %d from peer\n", rd->rmt_us[i].rkey,
			       rd->rmt_us[i].buf, rd->rmt_us[i].size);
		}
	}

	return 0;
}

static int server_recv(struct thread_data *td, struct ibv_wc *wc)
{
	struct rdma_rw_data *rd = td->io_ops_data;
	unsigned int max_bs;

	if (wc->wr_id == FIO_RDMA_MAX_IO_DEPTH) {
		rd->rdma_protocol = ntohl(rd->recv_buf.mode);

		max_bs = max(td->o.max_bs[DDIR_READ], td->o.max_bs[DDIR_WRITE]);
		if (max_bs < ntohl(rd->recv_buf.max_bs)) {
			log_err("fio: Server's block size (%d) must be greater than or "
				"equal to the client's block size (%d)!\n",
				ntohl(rd->recv_buf.max_bs), max_bs);
			return 1;
		}
	}

	return 0;
}

static int flush_buffer(struct thread_data *td)
{
	struct rdma_rw_data *rd = td->io_ops_data;
	off_t cur_pos;
	
	cur_pos = lseek(rd->backend_log_fd, 0, SEEK_CUR);
	if (cur_pos + rd->flushsize >= rd->backend_log_size)
		cur_pos = 0;
	lseek(rd->backend_log_fd, cur_pos, SEEK_SET);

	if (write(rd->backend_log_fd, rd->flush_buf, rd->flushsize) != rd->flushsize) {
		log_err("fio: flush log file: %m\n");
		return -1;
	}

	return 0;
}

static int fio_rdma_rw_client_sync_flush(struct thread_data *td)
{
	struct rdma_rw_data *rd = td->io_ops_data;
	struct ibv_wc flush_wc;
	struct ibv_cq *ev_cq;
	struct ibv_send_wr *bad_send_wr;
	struct ibv_recv_wr *bad_recv_wr;
	void *ev_ctx;
	int cnt = 0;

	rd->flush_turn++;

	ibv_post_recv(rd->flush_qp, &rd->flush_rq_wr, &bad_recv_wr);

	rd->flush_send_buf.flush_turn = rd->flush_turn;
	ibv_post_send(rd->flush_qp, &rd->flush_sq_wr, &bad_send_wr);

again:
	ibv_get_cq_event(rd->flush_comp_channel, &ev_cq, &ev_ctx);
	ibv_req_notify_cq(rd->flush_cq, 0);
	while (ibv_poll_cq(rd->flush_cq, 1, &flush_wc) == 1) {
		cnt++;
	}
	if (cnt != 2)
		goto again;

	return 0;
}

static int fio_rdma_rw_server_sync_flush(struct thread_data *td)
{
	struct rdma_rw_data *rd = td->io_ops_data;
	struct ibv_wc flush_wc;
	struct ibv_cq *ev_cq;
	struct ibv_send_wr *bad_send_wr;
	struct ibv_recv_wr *bad_recv_wr;
	void *ev_ctx;

	rd->flush_turn++;

	ibv_get_cq_event(rd->flush_comp_channel, &ev_cq, &ev_ctx);
	ibv_req_notify_cq(rd->flush_cq, 0);
	if (ibv_poll_cq(rd->flush_cq, 1, &flush_wc) != 1) {
		log_err("fio: ibv_poll_cq failed: %m\n");
		return -1;
	}
	if (rd->flush_recv_buf.opcode == END_WR_ID) {
		log_info("fio: server received termination from client\n");
		return 1;
	}

	if (rd->flush_recv_buf.opcode != FLUSH_WR_ID) {
		log_err("fio: server receives buggy flush request, "
			"want %d but received %d\n", FLUSH_WR_ID, rd->flush_recv_buf.opcode);
		return -1;
	}

	if (rd->flush_turn != rd->flush_recv_buf.flush_turn) {
		log_err("fio: flush turn mismatch!\n");
		return -1;
	}

	if (flush_buffer(td))
		return -1;

	ibv_post_recv(rd->flush_qp, &rd->flush_rq_wr, &bad_recv_wr);

	ibv_post_send(rd->flush_qp, &rd->flush_sq_wr, &bad_send_wr);
	ibv_get_cq_event(rd->flush_comp_channel, &ev_cq, &ev_ctx);
	ibv_req_notify_cq(rd->flush_cq, 0);
	if (ibv_poll_cq(rd->flush_cq, 1, &flush_wc) != 1) {
		log_err("fio: ibv_poll_cq failed: %m\n");
		return -1;
	}

	return 0;
}


static int cq_event_handler(struct thread_data *td, enum ibv_wc_opcode opcode)
{
	struct rdma_rw_data *rd = td->io_ops_data;
	struct ibv_wc wc;
	struct rdma_rw_u_data *r_io_u_d;
	int ret;
	int compevnum = 0;
	int i;

	while ((ret = ibv_poll_cq(rd->cq, 1, &wc)) == 1) {
		ret = 0;
		compevnum++;

		if (wc.status) {
			log_err("fio: cq completion status %d(%s)\n",
				wc.status, ibv_wc_status_str(wc.status));
			return -1;
		}

		switch (wc.opcode) {

		case IBV_WC_RECV:
			if (rd->is_client == 1)
				ret = client_recv(td, &wc);
			else
				ret = server_recv(td, &wc);

			if (ret)
				return -1;

			if (wc.wr_id == FIO_RDMA_MAX_IO_DEPTH)
				break;

			break;

		case IBV_WC_SEND:
		case IBV_WC_RDMA_WRITE:
		case IBV_WC_RDMA_READ:
			if (wc.wr_id == FIO_RDMA_MAX_IO_DEPTH)
				break;

			// find the io_u in flight that matches the wr_id
			for (i = 0; i < rd->io_u_flight_nr; i++) {
				r_io_u_d = rd->io_us_flight[i]->engine_data;

				if (wc.wr_id == r_io_u_d->sq_wr.wr_id) {
					rd->io_us_completed[rd->io_u_completed_nr] 
							= rd->io_us_flight[i];
					rd->io_u_completed_nr++;
					break;
				}
			}
			if (i == rd->io_u_flight_nr)
				log_err("fio: send wr %" PRId64 " not found\n",
					wc.wr_id);
			else {
				/* put the last one into middle of the list */
				rd->io_us_flight[i] =
				    rd->io_us_flight[rd->io_u_flight_nr - 1];
				rd->io_u_flight_nr--;

				rd->cached_len += r_io_u_d->sq_wr.sg_list->length;
				if (rd->cached_len >= rd->flushsize) {
					// log_info("client triggers flush, flushsize=%lu, cached_size=%lu\n", rd->flushsize,
					// 	rd->cached_len);
					if (fio_rdma_rw_client_sync_flush(td)) {
						log_err("fio: client sync flush failed\n");
						return -1;
					}

					rd->cached_len = 0;
				}
			}

			break;

		default:
			log_info("fio: unknown completion event %d\n",
				 wc.opcode);
			return -1;
		}
		rd->cq_event_num++;
	}

	if (ret) {
		log_err("fio: poll error %d\n", ret);
		return 1;
	}

	return compevnum;
}

/*
 * Return -1 for error and 'nr events' for a positive number
 * of events
 */
static int rdma_poll_wait(struct thread_data *td, enum ibv_wc_opcode opcode)
{
	struct rdma_rw_data *rd = td->io_ops_data;
	struct ibv_cq *ev_cq;
	void *ev_ctx;
	int ret;

	if (rd->cq_event_num > 0) {	/* previous left */
		rd->cq_event_num--;
		return 0;
	}

again:
	// may block
	if (ibv_get_cq_event(rd->channel, &ev_cq, &ev_ctx) != 0) {
		log_err("fio: Failed to get cq event!\n");
		return -1;
	}
	if (ev_cq != rd->cq) {
		log_err("fio: Unknown CQ!\n");
		return -1;
	}
	if (ibv_req_notify_cq(rd->cq, 0) != 0) {
		log_err("fio: Failed to set notify!\n");
		return -1;
	}

	ret = cq_event_handler(td, opcode);
	if (ret == 0)
		goto again;

	ibv_ack_cq_events(rd->cq, ret);

	rd->cq_event_num--;

	return ret;
}

// static int flush_cq_event_handler(struct thread_data *td)
// {
// 	struct rdma_rw_data *rd = td->io_ops_data;
// 	struct ibv_wc wc, flush_wc;
// 	int ret;
// 	int compevnum = 0;
// 	int i;
// 	struct ibv_send_wr *bad_send_wr;
// 	struct ibv_recv_wr *bad_recv_wr;

// 	while ((ret = ibv_poll_cq(rd->flush_cq, 1, &wc)) == 1) {
// 		ret = 0;
// 		compevnum++;

// 		if (wc.status) {
// 			log_err("fio: cq completion status %d(%s)\n",
// 				wc.status, ibv_wc_status_str(wc.status));
// 			return -1;
// 		}

// 		switch (wc.opcode) {
// 		case IBV_WC_RECV:

// 		}
// 	}
// }

// static int fio_rdma_rw_poll_wait_flush(struct thread_data *td)
// {
// 	struct rdma_rw_data *rd = td->io_ops_data;
// 	struct ibv_cq *ev_cq;
// 	void *ev_ctx;
// 	int ret;

// 	if (rd->cq_event_num > 0) {	/* previous left */
// 		rd->cq_event_num--;
// 		return 0;
// 	}

// again:
// 	// may block
// 	if (ibv_get_cq_event(rd->flush_comp_channel, &ev_cq, &ev_ctx) != 0) {
// 		log_err("fio: Failed to get cq event!\n");
// 		return -1;
// 	}
// 	if (ev_cq != rd->cq) {
// 		log_err("fio: Unknown CQ!\n");
// 		return -1;
// 	}
// 	if (ibv_req_notify_cq(rd->cq, 0) != 0) {
// 		log_err("fio: Failed to set notify!\n");
// 		return -1;
// 	}

// 	ret = flush_cq_event_handler(td);
// 	if (ret == 0)
// 		goto again;

// 	ibv_ack_cq_events(rd->cq, 1);

// 	rd->cq_event_num--;

// 	return ret;
// }

static int fio_rdma_rw_accept(struct thread_data *td, struct fio_file *f)
{
	struct rdma_rw_data *rd = td->io_ops_data;
	struct rdma_conn_param conn_param;
	struct ibv_send_wr *bad_sq_wr;
	struct ibv_recv_wr *bad_cq_wr;
	int ret = 0;

	/* rdma_accept() - then wait for accept success */
	memset(&conn_param, 0, sizeof(conn_param));
	conn_param.responder_resources = 1;
	conn_param.initiator_depth = 1;

	if (rdma_accept(rd->child_cm_id, &conn_param) != 0) {
		log_err("fio: rdma_accept: %m\n");
		return 1;
	}

	if (get_next_channel_event
	    (td, rd->cm_channel, RDMA_CM_EVENT_ESTABLISHED, 0) != 0) {
		log_err("fio: wait for RDMA_CM_EVENT_ESTABLISHED\n");
		return 1;
	}

	/* wait for request */
	ret = rdma_poll_wait(td, IBV_WC_RECV) < 0;

	if (ibv_post_send(rd->qp, &rd->sq_wr, &bad_sq_wr) != 0) {
		log_err("fio: ibv_post_send fail: %m\n");
		return 1;
	}

	if (rdma_poll_wait(td, IBV_WC_SEND) < 0)
		return 1;

	memset(&conn_param, 0, sizeof(conn_param));
	conn_param.responder_resources = 1;
	conn_param.initiator_depth = 1;

	if (fio_rdma_rw_server_setup_flush(td))
		return 1;

	if (rdma_accept(rd->flush_cm_id, &conn_param) != 0) {
		log_err("fio: rdma_accept: %m\n");
		return 1;
	}

	if (get_next_channel_event
	    (td, rd->flush_cm_channel, RDMA_CM_EVENT_ESTABLISHED, 0) != 0) {
		log_err("fio: wait for RDMA_CM_EVENT_ESTABLISHED\n");
		return 1;
	}

	// post a recv for flush request
	if (ibv_post_recv(rd->flush_qp, &rd->flush_rq_wr, &bad_cq_wr) != 0) {
		log_err("fio: ibv_post_recv for flush fail: %m\n");
		return 1;
	}

	return ret;
}

static int fio_rdma_rw_connect(struct thread_data *td, struct fio_file *f)
{
	struct rdma_rw_data *rd = td->io_ops_data;
	struct rdma_conn_param conn_param;
	struct ibv_send_wr *bad_wr;

	memset(&conn_param, 0, sizeof(conn_param));
	conn_param.responder_resources = 1;
	conn_param.initiator_depth = 1;
	conn_param.retry_count = 10;

	if (rdma_connect(rd->cm_id, &conn_param) != 0) {
		log_err("fio: rdma_connect fail: %m\n");
		return 1;
	}

	if (get_next_channel_event
	    (td, rd->cm_channel, RDMA_CM_EVENT_ESTABLISHED, 0) != 0) {
		log_err("fio: wait for RDMA_CM_EVENT_ESTABLISHED\n");
		return 1;
	}

	/* send task request */
	rd->send_buf.mode = htonl(rd->rdma_protocol);
	rd->send_buf.nr = htonl(td->o.iodepth);

	if (ibv_post_send(rd->qp, &rd->sq_wr, &bad_wr) != 0) {
		log_err("fio: ibv_post_send fail: %m\n");
		return 1;
	}

	if (rdma_poll_wait(td, IBV_WC_SEND) < 0)
		return 1;

	/* wait for remote MR info from server side */
	if (rdma_poll_wait(td, IBV_WC_RECV) < 0)
		return 1;

	usleep(500000);

	memset(&conn_param, 0, sizeof(conn_param));
	conn_param.responder_resources = 1;
	conn_param.initiator_depth = 1;
	conn_param.retry_count = 10;

	if (rdma_connect(rd->flush_cm_id, &conn_param) != 0) {
		log_err("fio: rdma_connect fail: %m\n");
		return 1;
	}

	if (get_next_channel_event
	    (td, rd->flush_cm_channel, RDMA_CM_EVENT_ESTABLISHED, 1) != 0) {
		log_err("fio: wait for RDMA_CM_EVENT_ESTABLISHED\n");
		return 1;
	}

	/* In SEND/RECV test, it's a good practice to setup the iodepth of
	 * of the RECV side deeper than that of the SEND side to
	 * avoid RNR (receiver not ready) error. The
	 * SEND side may send so many unsolicited message before
	 * RECV side commits sufficient recv buffers into recv queue.
	 * This may lead to RNR error. Here, SEND side pauses for a while
	 * during which RECV side commits sufficient recv buffers.
	 */
	usleep(300000);

	return 0;
}

static int open_backend_log(struct thread_data *td) {
	struct rdma_rw_data *rd = td->io_ops_data;
	struct stat st;
	char *filename = td->o.filename;

	rd->backend_log_fd = open(filename, O_WRONLY | O_DIRECT);
	if (rd->backend_log_fd < 0) {
		log_err("fio: open fail: %m\n");
		return -1;
	}

	if (fstat(rd->backend_log_fd, &st)) {
		log_err("fio: fstat fail: %m\n");
		return -1;
	}

	rd->backend_log_size = st.st_size;

	return 0;
}

static int fio_rdma_rw_open_file(struct thread_data *td, struct fio_file *f)
{
	int ret;
	if (td_read(td)) {
		ret = fio_rdma_rw_accept(td, f);
		if (ret)
			return ret;
		return open_backend_log(td);
	} else
		return fio_rdma_rw_connect(td, f);
}


/*
 * The ->prep() function is called for each io_u prior to being submitted
 * with ->queue(). This hook allows the io engine to perform any
 * preparatory actions on the io_u, before being submitted. Not required.
 */
static int fio_rdma_rw_prep(struct thread_data *td, struct io_u *io_u)
{
	struct rdma_rw_data *rd = td->io_ops_data;
	struct rdma_rw_u_data *r_io_u_d;

	r_io_u_d = io_u->engine_data;

	switch (rd->rdma_protocol) {
	case FIO_RDMA_RW_WRITE:
	case FIO_RDMA_RW_READ:
		r_io_u_d->rdma_sgl.addr = (uint64_t) (unsigned long)io_u->buf;
		r_io_u_d->rdma_sgl.lkey = io_u->mr->lkey;
		r_io_u_d->sq_wr.wr_id = r_io_u_d->wr_id;
		r_io_u_d->sq_wr.send_flags = IBV_SEND_SIGNALED;
		r_io_u_d->sq_wr.sg_list = &r_io_u_d->rdma_sgl;
		r_io_u_d->sq_wr.num_sge = 1;
		break;
	default:
		log_err("fio: unknown rdma protocol - %d\n", rd->rdma_protocol);
		break;
	}

	return 0;
}

/*
 * The ->event() hook is called to match an event number with an io_u.
 * After the core has called ->getevents() and it has returned eg 3,
 * the ->event() hook must return the 3 events that have completed for
 * subsequent calls to ->event() with [0-2]. Required.
 */
static struct io_u *fio_rdma_rw_event(struct thread_data *td, int event)
{
	struct rdma_rw_data *rd = td->io_ops_data;
	struct io_u *io_u;
	int i;

	io_u = rd->io_us_completed[0];
	for (i = 0; i < rd->io_u_completed_nr - 1; i++)
		rd->io_us_completed[i] = rd->io_us_completed[i + 1];

	rd->io_u_completed_nr--;

	dprint_io_u(io_u, "fio_rdmaio_event");

	return io_u;
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
	struct rdma_rw_data *rd = td->io_ops_data;
	enum ibv_wc_opcode comp_opcode;
	struct ibv_cq *ev_cq;
	void *ev_ctx;
	int ret, r = 0;
	comp_opcode = IBV_WC_RDMA_WRITE;

	switch (rd->rdma_protocol) {
	case FIO_RDMA_RW_WRITE:
		comp_opcode = IBV_WC_RDMA_WRITE;
		break;
	case FIO_RDMA_RW_READ:
		comp_opcode = IBV_WC_RDMA_READ;
		break;
	default:
		log_err("fio: unknown rdma protocol - %d\n", rd->rdma_protocol);
		break;
	}

	if (rd->cq_event_num > 0) {	/* previous left */
		rd->cq_event_num--;
		return 0;
	}

again:
	if (ibv_get_cq_event(rd->channel, &ev_cq, &ev_ctx) != 0) {
		log_err("fio: Failed to get cq event!\n");
		return -1;
	}
	if (ev_cq != rd->cq) {
		log_err("fio: Unknown CQ!\n");
		return -1;
	}
	if (ibv_req_notify_cq(rd->cq, 0) != 0) {
		log_err("fio: Failed to set notify!\n");
		return -1;
	}

	ret = cq_event_handler(td, comp_opcode);
	if (ret < 0)
		return -1;

	if (ret < 1)
		goto again;

	ibv_ack_cq_events(rd->cq, ret);

	r += ret;
	if (r < min)
		goto again;

	rd->cq_event_num -= r;

	return r;
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
	struct rdma_rw_data *rd = td->io_ops_data;

	fio_ro_check(td, io_u);

	if (rd->io_u_queued_nr == (int)td->o.iodepth)
		return FIO_Q_BUSY;

	rd->io_us_queued[rd->io_u_queued_nr] = io_u;
	rd->io_u_queued_nr++;

	dprint_io_u(io_u, "fio_rdmaio_queue");

	return FIO_Q_QUEUED;
}

static int fio_rdma_rw_send(struct thread_data *td, struct io_u **io_us,
			   unsigned int nr)
{
	struct rdma_rw_data *rd = td->io_ops_data;
	struct ibv_send_wr *bad_wr;
	int i;
	long index;
	struct rdma_rw_u_data *r_io_u_d;

	r_io_u_d = NULL;

	// consume the io_u in rd->io_us_queued, nr = io_u_queued_nr
	for (i = 0; i < nr; i++) {
		/* RDMA_WRITE or RDMA_READ */
		switch (rd->rdma_protocol) {
		case FIO_RDMA_RW_WRITE:
			/* compose work request */
			r_io_u_d = io_us[i]->engine_data;
			index = __rand(&rd->rand_state) % rd->rmt_nr;
			r_io_u_d->sq_wr.opcode = IBV_WR_RDMA_WRITE;
			r_io_u_d->sq_wr.wr.rdma.rkey = rd->rmt_us[index].rkey;
			r_io_u_d->sq_wr.wr.rdma.remote_addr = \
				rd->rmt_us[index].buf;
			r_io_u_d->sq_wr.sg_list->length = io_us[i]->buflen;
			break;
		case FIO_RDMA_RW_READ:
			/* compose work request */
			r_io_u_d = io_us[i]->engine_data;
			index = __rand(&rd->rand_state) % rd->rmt_nr;
			r_io_u_d->sq_wr.opcode = IBV_WR_RDMA_READ;
			r_io_u_d->sq_wr.wr.rdma.rkey = rd->rmt_us[index].rkey;
			r_io_u_d->sq_wr.wr.rdma.remote_addr = \
				rd->rmt_us[index].buf;
			r_io_u_d->sq_wr.sg_list->length = io_us[i]->buflen;
			break;
		default:
			log_err("fio: unknown rdma protocol - %d\n",
				rd->rdma_protocol);
			break;
		}

		if (ibv_post_send(rd->qp, &r_io_u_d->sq_wr, &bad_wr) != 0) {
			log_err("fio: ibv_post_send fail: %m\n");
			return -1;
		}

		dprint_io_u(io_us[i], "fio_rdmaio_send");
	}

	/* wait for completion
	   rdma_poll_wait(td, comp_opcode); */

	return i;
}

static int fio_rdma_rw_recv(struct thread_data *td, struct io_u **io_us,
			   unsigned int nr)
{
	int ret;
	while (true) {
		// log_info("server flushes\n");
		ret = fio_rdma_rw_server_sync_flush(td);
		if (ret == -1) {
			log_err("fio: server sync flush failed\n");
			goto err;
		} else if (ret == 1) {
			break;
		}
	}

	dprint(FD_IO, "fio: recv FINISH message\n");
	td->done = 1;
	return 0;

err:
	td->done = 1;
	return -1;
}

static void fio_rdma_rw_queued(struct thread_data *td, struct io_u **io_us,
			      unsigned int nr)
{
	struct rdma_rw_data *rd = td->io_ops_data;
	struct timespec now;
	unsigned int i;

	if (!fio_fill_issue_time(td))
		return;

	fio_gettime(&now, NULL);

	for (i = 0; i < nr; i++) {
		struct io_u *io_u = io_us[i];

		/* queued -> flight */
		rd->io_us_flight[rd->io_u_flight_nr] = io_u;
		rd->io_u_flight_nr++;

		memcpy(&io_u->issue_time, &now, sizeof(now));
		io_u_queued(td, io_u);
	}

	/*
	 * only used for iolog
	 */
	if (td->o.read_iolog_file)
		memcpy(&td->last_issue, &now, sizeof(now));
}

static int fio_rdma_rw_commit(struct thread_data *td) {
	struct rdma_rw_data *rd = td->io_ops_data;
	struct io_u **io_us;
	int ret;

	if (!rd->io_us_queued)
		return 0;

	io_us = rd->io_us_queued;
	do {
		/* RDMA_WRITE or RDMA_READ */
		if (rd->is_client)
			ret = fio_rdma_rw_send(td, io_us, rd->io_u_queued_nr);
		else if (!rd->is_client)
			ret = fio_rdma_rw_recv(td, io_us, rd->io_u_queued_nr);
		else
			ret = 0;	/* must be a SYNC */

		if (ret > 0) {
			fio_rdma_rw_queued(td, io_us, ret);
			io_u_mark_submit(td, ret);
			rd->io_u_queued_nr -= ret;
			io_us += ret;
			ret = 0;
		} else
			break;
	} while (rd->io_u_queued_nr);

	return ret;
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
 * Hook for closing a file. See fio_skeleton_open().
 */
static int fio_rdma_rw_close_file(struct thread_data *td, struct fio_file *f)
{
	struct rdma_rw_data *rd = td->io_ops_data;
	struct ibv_send_wr *bad_wr;

	/* unregister rdma buffer */

	/*
	 * Client sends notification to the server side
	 */
	/* refer to: http://linux.die.net/man/7/rdma_cm */
	if ((rd->is_client == 1) && ((rd->rdma_protocol == FIO_RDMA_RW_READ)
				     || (rd->rdma_protocol == FIO_RDMA_RW_WRITE))) {
		rd->flush_send_buf.opcode = END_WR_ID;
		if (ibv_post_send(rd->flush_qp, &rd->flush_sq_wr, &bad_wr) != 0) {
			log_err("fio: ibv_post_send fail: %m\n");
			return 1;
		}

		dprint(FD_IO, "fio: close information sent success\n");
	}

	if (rd->is_client == 1) {
		rdma_disconnect(rd->cm_id);
		rdma_disconnect(rd->flush_cm_id);
	} else {
		rdma_disconnect(rd->child_cm_id);
		rdma_disconnect(rd->flush_cm_id);
	}


	ibv_destroy_cq(rd->cq);
	ibv_destroy_cq(rd->flush_cq);

	ibv_destroy_qp(rd->qp);
	ibv_destroy_qp(rd->flush_qp);

	if (rd->is_client == 1) {
		rdma_destroy_id(rd->cm_id);
		rdma_destroy_id(rd->flush_cm_id);
	} else {
		rdma_destroy_id(rd->child_cm_id);
		rdma_destroy_id(rd->cm_id);
		rdma_destroy_id(rd->flush_listen_cm_id);
		rdma_destroy_id(rd->flush_cm_id);
	}

	ibv_destroy_comp_channel(rd->channel);
	ibv_destroy_comp_channel(rd->flush_comp_channel);
	ibv_dealloc_pd(rd->pd);
	ibv_dealloc_pd(rd->flush_pd);

	return 0;
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
	.post_init	= fio_rdma_rw_post_init,
	.prep		= fio_rdma_rw_prep,
	.queue		= fio_rdma_rw_queue,
	.commit		= fio_rdma_rw_commit,
	.getevents	= fio_rdma_rw_getevents,
	.event		= fio_rdma_rw_event,
	.cleanup	= fio_rdma_rw_cleanup,
	.open_file	= fio_rdma_rw_open_file,
	.close_file	= fio_rdma_rw_close_file,
	.flags		= FIO_DISKLESSIO | FIO_UNIDIR | FIO_PIPEIO |
					FIO_ASYNCIO_SETS_ISSUE_TIME,
	.options	= options,
	.option_struct_size	= sizeof(struct rdma_rw_options),
};
