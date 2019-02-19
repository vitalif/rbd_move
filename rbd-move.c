#include <stdio.h>
#include <errno.h>
#include <string.h>
#include "rados/librados.h"
#include "rbd/librbd.h"

struct rbd_move_data
{
    rbd_image_t src;
    rbd_image_t dst;
};

int move_extent(uint64_t offset, size_t len, const char *buf, void* data_ptr)
{
    struct rbd_move_data* data = (struct rbd_move_data*)data_ptr;
    if (buf != NULL)
    {
        int err;
        err = rbd_write2(data->dst, offset, len, buf, LIBRADOS_OP_FLAG_FADVISE_SEQUENTIAL);
        if (err < 0)
            return err;
        err = rbd_discard(data->src, offset, len);
        if (err < 0)
            return err;
    }
    return 0;
}

void rbd_move(rbd_image_t src, rbd_image_t dst, uint64_t size)
{
    struct rbd_move_data data = {
        src,
        dst,
    };
    rbd_read_iterate2(src, 0, size, move_extent, &data);
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
                            rbd_move(src, dst, size);
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
