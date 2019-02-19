#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include "rados/librados.h"
#include "rbd/librbd.h"

#define MOVE_BUFFER 0x4000000

int rbd_move(rbd_image_t src, rbd_image_t dst, uint64_t size)
{
    int err = 0, i;
    uint64_t cur = 0, len, prev_offset = 0, prev_len = 0;
    rbd_completion_t read_comp = NULL, write_comp = NULL, discard_comp = NULL;
    void *read_buffer = malloc(MOVE_BUFFER*2);
    if (!read_buffer)
    {
        return 1;
    }
    void *curbuf = read_buffer;
    while (cur < size)
    {
        printf("\r%lld MB / %lld MB...", cur/1024/1024, size/1024/1024);
        rbd_completion_t comp;
        err = rbd_aio_create_completion(NULL, NULL, &read_comp);
        if (err < 0)
            goto ret;
        len = size-cur > MOVE_BUFFER ? MOVE_BUFFER : size-cur;
        err = rbd_aio_read(src, cur, len, curbuf, read_comp);
        if (err < 0)
            goto ret;
        err = rbd_aio_wait_for_complete(read_comp);
        if (err < 0)
            goto ret;
        rbd_aio_release(read_comp);
        for (i = 0; i < MOVE_BUFFER/8; i++)
        {
            if (((uint64_t*)curbuf)[i] != 0)
            {
                if (write_comp)
                {
                    err = rbd_aio_wait_for_complete(write_comp);
                    if (err < 0)
                        goto ret;
                    rbd_aio_release(write_comp);
                    if (discard_comp)
                    {
                        err = rbd_aio_wait_for_complete(discard_comp);
                        if (err < 0)
                            goto ret;
                        rbd_aio_release(discard_comp);
                    }
                    err = rbd_aio_create_completion(NULL, NULL, &discard_comp);
                    if (err < 0)
                        goto ret;
                    err = rbd_aio_discard(src, prev_offset, prev_len, discard_comp);
                    if (err < 0)
                        goto ret;
                }
                err = rbd_aio_create_completion(NULL, NULL, &write_comp);
                if (err < 0)
                    goto ret;
                err = rbd_aio_write(dst, cur, len, curbuf, write_comp);
                if (err < 0)
                    goto ret;
                curbuf = curbuf == read_buffer ? read_buffer+MOVE_BUFFER : read_buffer;
                prev_offset = cur;
                prev_len = len;
                break;
            }
        }
        cur += MOVE_BUFFER;
    }
    if (write_comp)
    {
        err = rbd_aio_wait_for_complete(write_comp);
        if (err < 0)
            goto ret;
        rbd_aio_release(write_comp);
        write_comp = NULL;
        if (discard_comp)
        {
            err = rbd_aio_wait_for_complete(discard_comp);
            if (err < 0)
                goto ret;
            rbd_aio_release(discard_comp);
            discard_comp = NULL;
        }
        err = rbd_discard(src, prev_offset, prev_len);
        if (err < 0)
            goto ret;
    }
    printf(" Done\n");
ret:
    free(read_buffer);
    return 0;
}

int main(int argc, char **argv)
{
    char *pool_name, *src_img, *dst_img;
    int err;
    int st;
    rados_t cluster;
    rados_ioctx_t io;
    rbd_image_t src, dst;
    uint64_t size;
    if (argc < 4)
    {
        printf("USAGE: ./rbd-move POOL FROM TO\n");
        return 1;
    }
    pool_name = argv[1];
    src_img = argv[2];
    dst_img = argv[3];
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
