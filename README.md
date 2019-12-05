# chum - mako load generator

`chum` is a load generator for an individual manta-mako server.

## How it works

`chum` creates a number of threads. Each of these threads will synchronously
upload files to the target server using an HTTP PUT. The data uploaded is a
chunk of random bytes.

Upload file size distribution is an important part of how `chum` works. `chum`
includes a default object size distribution if one is not provided at the CLI.
The CLI option for specifying a file size distribution is `-d`. When a `chum`
thread uploads a file it will choose a random file size from the provided
size distribution list. To skew the result of the distribution more of a given
size should be specified at the CLI.

For example, take the file size distribution [128, 256, 512]. These values are
interpreted to be kilobytes by default. Over the course of three file upload
loops a single `chum` thread will choose from this distribution randomly and
upload files. Maybe the first upload was chosen to be 512k, the second was
128k, and the third was 512k.

Now let's say that we want to simulate a workload where 80% of uploads are
128k, and the remaining 20% are 512k and 1m. We can use a distribution like
[128, 128, 128, 128, 128, 128, 128, 128, 512, 1024]. Assuming the random
selection is truly random in the limit, 8/10 uploads will be 128k in size,
1/10 will be 512k in size, and 1/10 will be 1m in size.

There are two ways to specify the previous distribution at the CLI.

Long form:
```
-d 128,128,128,128,128,128,128,128,512,1024 
```
Short form:
```
-d 128x8,512,1024
```

Using the short form, a given size AxB is interpreted as 'add B copies of
A to the distribution.' The long and short form examples provided result in
equivalent distributions.

## Running

First, make sure that the 'chum' directory is created in the file server root.
This is where `chum` writes files. For nginx the directory must be owned by
`nobody:nobody`.

```
(nginx) $ mkdir /manta/chum
(nginx) $ chown nobody:nobody /manta/chum
```

Getting help:

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
    -i, --interval NUM  interval in seconds at which to report stats, default:
                        2
    -v, --verbose       enable per-thread stat reporting
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
