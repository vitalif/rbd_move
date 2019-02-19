#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include "rados/librados.h"
#include "rbd/librbd.h"

// Move 8 objects at a time
#define MOVE_BY 8

struct rbd_move_data
{
    rbd_image_t src;
    rbd_image_t dst;
    int read_batch;
    uint64_t read_offset[MOVE_BY];
    size_t read_len[MOVE_BY];
    void *buffers[MOVE_BY];
    size_t buffer_size[MOVE_BY];
};

int move_extent_apply(struct rbd_move_data *data)
{
    if (data->read_batch > 0)
    {
        rbd_completion_t comp[MOVE_BY];
        int i, err;
        for (i = 0; i < data->read_batch; i++)
        {
            if (data->buffer_size[i] < data->read_len[i])
            {
                data->buffers[i] = realloc(data->buffers[i], data->read_len[i]);
                data->buffer_size[i] = data->read_len[i];
            }
            err = rbd_aio_create_completion(NULL, NULL, &comp[i]);
            if (err < 0)
                return err;
            err = rbd_aio_read(data->src, data->read_offset[i], data->read_len[i], data->buffers[i], comp[i]);
            if (err < 0)
                return err;
        }
        for (i = 0; i < data->read_batch; i++)
        {
            err = rbd_aio_wait_for_complete(comp[i]);
            if (err < 0)
                return err;
            rbd_aio_release(comp[i]);
            err = rbd_aio_create_completion(NULL, NULL, &comp[i]);
            if (err < 0)
                return err;
            err = rbd_aio_write(data->dst, data->read_offset[i], data->read_len[i], data->buffers[i], comp[i]);
            if (err < 0)
                return err;
        }
        for (i = 0; i < data->read_batch; i++)
        {
            err = rbd_aio_wait_for_complete(comp[i]);
            if (err < 0)
                return err;
            rbd_aio_release(comp[i]);
            err = rbd_aio_create_completion(NULL, NULL, &comp[i]);
            if (err < 0)
                return err;
            err = rbd_aio_discard(data->src, data->read_offset[i], data->read_len[i], comp[i]);
            if (err < 0)
                return err;
        }
        for (i = 0; i < data->read_batch; i++)
        {
            err = rbd_aio_wait_for_complete(comp[i]);
            if (err < 0)
                return err;
            rbd_aio_release(comp[i]);
        }
        data->read_batch = 0;
    }
    return 0;
}

int move_extent(uint64_t offset, size_t len, int exists, void* data_ptr)
{
    int err, i;
    struct rbd_move_data* data = (struct rbd_move_data*)data_ptr;
    if (exists)
    {
        if (data->read_batch >= MOVE_BY)
        {
            fprintf(stderr, "Extent count exceeded\n");
            exit(1);
        }
        data->read_offset[data->read_batch] = offset;
        data->read_len[data->read_batch] = len;
        data->read_batch++;
    }
    return 0;
}

int rbd_move(rbd_image_t src, rbd_image_t dst, uint64_t size)
{
    int err;
    uint64_t cur = 0;
    struct rbd_move_data data = { 0 };
    data.src = src;
    data.dst = dst;
    while (cur < size)
    {
        printf("\r%lld MB / %lld MB...", cur/1024/1024, size/1024/1024);
        err = rbd_diff_iterate2(
            src, NULL, cur, size-cur > MOVE_BY*4*1024*1024 ? MOVE_BY*4*1024*1024 : size-cur,
            1, 1, move_extent, &data
        );
        if (err < 0)
        {
            return err;
        }
        move_extent_apply(&data);
        cur += MOVE_BY*4*1024*1024;
    }
    printf(" Done\n");
    return 0;
}

int main(int argc, char **argv)
{
    char *pool_name = "rpool";
    char *src_img = "one-1-49-1";
    char *dst_img = "one-1-49-1-ec";
    int err;
    int st;
    rados_t cluster;
    rados_ioctx_t io;
    rbd_image_t src, dst;
    uint64_t size;
    setvbuf(stdout, NULL, _IONBF, 0);
    err = rados_create(&cluster, NULL);
    if (err < 0)
    {
        fprintf(stderr, "%s: cannot create a cluster handle: %s\n", argv[0], strerror(-err));
        st = 1;
    }
    else
    {
        err = rados_conf_read_file(cluster, "/etc/ceph/ceph.conf");
        if (err < 0)
        {
            fprintf(stderr, "%s: cannot read config file: %s\n", argv[0], strerror(-err));
            st = 1;
        }
        else
        {
            err = rados_connect(cluster);
            if (err < 0)
            {
                fprintf(stderr, "%s: cannot connect to cluster: %s\n", argv[0], strerror(-err));
                st = 1;
            }
            else
            {
                err = rados_ioctx_create(cluster, pool_name, &io);
                if (err < 0)
                {
                    fprintf(stderr, "%s: cannot open rados pool %s: %s\n", argv[0], pool_name, strerror(-err));
                    st = 1;
                }
                else
                {
                    err = rbd_open(io, src_img, &src, NULL);
                    if (err < 0)
                    {
                        fprintf(stderr, "%s: cannot open image %s/%s: %s\n", argv[0], pool_name, src_img, strerror(-err));
                        st = 1;
                    }
                    else
                    {
                        err = rbd_open(io, dst_img, &dst, NULL);
                        if (err < 0)
                        {
                            fprintf(stderr, "%s: cannot open image %s/%s: %s\n", argv[0], pool_name, dst_img, strerror(-err));
                            st = 1;
                        }
                        else
                        {
                            err = rbd_get_size(src, &size);
                            if (err < 0)
                            {
                                fprintf(stderr, "%s: cannot stat %s/%s: %s\n", argv[0], pool_name, src_img, strerror(-err));
                                st = 1;
                            }
                            err = rbd_move(src, dst, size);
                            if (err < 0)
                            {
                                fprintf(stderr, "%s: failed to move data: %s\n", argv[0], strerror(-err));
                                st = 1;
                            }
                            rbd_close(dst);
                        }
                        rbd_close(src);
                    }
                    rados_ioctx_destroy(io);
                }
                rados_shutdown(cluster);
            }
        }
    }
    return st;
}
