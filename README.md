# chum - mako load generator

`chum` is a load generator for an individual manta-mako server.

## Running

```
$ chum -h
chum - Upload files to a given file server as quickly as possible

Options:
    -t, --target IP     target server
    -c, --concurrency NUM
                        number of concurrent threads, default: 1
    -p, --pause NUM     pause duration in millis between each upload, default:
                        0
    -d, --distribution NUM,NUM,...
                        comma-separated distribution of file sizes to upload,
                        default: [128, 256, 512]
    -u, --unit k|m      capacity unit for upload file size, default: k
    -h, --help          print this help message
```

A target is required at a minimum:
```
$ chum -t 127.0.0.1
```

Target a local nginx server, 50 worker threads, an object size distribution of
[1m, 2m, 3m], each thread pausing 1000ms between uploads:

```
$ chum -t 127.0.0.1 -c 50 -d 1,2,3 -u m -p 1000
```

## Building

On SmartOS we recommend using image `f3a6e1a2-9d71-11e9-9bd2-e7e5b4a5c141`,
a recent base-64 image.

The following packages are required to download and build `chum` in that base-64
zone. The can be installed via pkgsrc via `pkgin(1)`

```
build-essential git rust-1.35.0nb1
```

To build:
```
$ cd chum
$ cargo build
```

## License

"chum" is licensed under the
[Mozilla Public License version 2.0](http://mozilla.org/MPL/2.0/).
See the file LICENSE.
