# FTS

The CERN [File Transfer Service (FTS3)](https://fts3-docs.web.cern.ch/fts3-docs/) is a service built to manage data movement between grid storage elements. FTS3 is the service responsible for globally distributing the majority of the LHC data across the WLCG infrastructure. 

Grid storage endpoints support XRootD and WebDAV protocols. They also support third-party copies (TPC), by having functionality to connect directly to other grid storage endpoints and either read files from or write files to remote storage to/from another remote storage endpoint. There is no intermediate “resting place” for the data to stream through from the data source to the data destination.

For the Datastore API, FTS is used to move between a grid tape storage endpoint and lightweight disk storage endpoints created by running an XRootD server on top of a mounted filesystem (e.g. CephFS).

One of the core strengths of FTS3 is its ability to mediate transfers without sitting on the data path. This means the limiting factor of transfer speed between two storage elements is the storage bandwidth and network capacity. This makes it easy to build scalable data management systems without having to consider multi-threaded and multi-node architecture. 

User interaction with FTS3 is via the command line tools, the REST API or the python bindings. A user will specify a list of one or more files to transfer between storage elements, and FTS will return a job ID for the user to track the progress. FTS will mediate the transfers of the files between the storage endpoints, using third party copies (if supported). Failures will be retried, and files will be transferred in parallel. FTS will attempt to optimize job performance by increasing or decreasing number of parallel transfers in response to performance changes and errors.  

Another key strength of FTS in this proposed application is its ability to deal with tape endpoints. Transfers from a tape endpoint will be preceded by a file staging step managed by FTS, and when the file is on the tape disk buffer FTS will start the transfer to the destination, and finally evict the file from the tape buffer after the transfer has completed successfully. This takes a huge amount of complexity out of managing recalls from tape for users, who simply submit a job to move files from the tape storage to another storage endpoint and then monitor the progress. 

## Authentication and authorization

As FTS is a grid computing tool, the main authentication and authorisation mechanism supported is X509 client certificates. FTS will expect any job submission to have an attached X509 proxy – a short-lived self-signed certificate that can be delegated to the FTS service and then passed onto on to the storage endpoints. 

[FTS authentication and authorisation flow](fts_auth.png)

As the user credentials are passed onto the storage endpoints, the point where the authorisation happens is at the storage endpoints. As such, the FTS does not authorise requests, and will ‘blindly’ pass on data transfer requests, along with the provided credentials. This allows the FTS service to enforce very lightweight authorisation on users - provided requests have an appropriate authentication method attached (e.g. an X509 user proxy), they will be accepted, and the decision making is left up to storage endpoints whether to allow the request.

## Dedicated or shared

FTS is designed to support multiple user groups (or Virtual Organisations in FTS) on a single instance. One consideration here is if there is sensitive information in the storage endpoints and/or filenames used. The FTS web interface allows users to view all recent transfers submitted to the FTS instance without authentication. This is not an issue but is something to bear in mind.

[Example of the publicly accessible view into transfer history](fts_monitoring.png)

## Job sizes

Submitting a transfer job involves a POST request containing a JSON payload to the FTS service. As such there is limitation as to how many file transfers can be appended to a single job before the FTS service will reject the submission due to the request size.
Keeping the number of transfers per job below 1000 is a good rule of thumb, and designing a system that can submit batches of transfers and manage multiple job IDs is important.

As the exact limit is number of bytes rather than number of files, the length of file URLs and amount of attached metadata will also influence the maximum number of files per job.

## Offline storage

As described in the background section, FTS is designed to handle the hard work of interacting with a tape storage endpoint. For a user submitting jobs that are archiving or retrieving files on an offline storage system, there are a few things to consider depending on the direction of the transfer (archive or retrieval).

### Retrieval

When submitting a job for retrieval, the parameter ‘bring_online’ should be configured. This serves two purposes:

1.	Indicates to FTS that the file is not expected to be immediately readable, and that the transfer requests should be preceded by a staging request.
2.	Indicates how long FTS should wait for the file to be staged before considering the file as inaccessible and reporting the transfer as failed.

Recall timeouts should be set in the order of days, although most recalls can be expected to complete within an hour under normal operations.

### Archival

Recent functionality enabled FTS to monitor files transferred to a tape backed storage endpoint to be monitored for successful archival before reporting the transfer as ‘FINISHED’. This is achieved by monitoring the file stat flags and looking for the presence of the ‘BackupExists’ flag, which is only true after the file has been transferred to tape. The file will be reported as being in an ‘ARCHIVING’ state whilst this is ongoing.

Enabling this is done by setting the ‘archive_timeout’ flag to a non-negative value. As with the recall timeout (bring_online), this both enables the ‘wait for archival’ functionality and configures the amount of time that will be waited without a successful archival before the transfer is considered failed.

## Verification and Reconciliation 

The FTS service can report the state of a transferred file to a high degree of certainty and can retry failed transfers. However, it is not designed to check the state of a file at a storage without an associated transfer job.

Therefore, ensuring you have a method of validating that the files present in your metadata catalogue match the physical files present on the archive is important. 
For offline files, FTS will not check if the file is present on the destination and fail the transfer until the file is staged on the source. This can result in expensive and unnecessary recall operations being carried out for files that are already present on the destination disk. Checking which files exist on the destination before submitting the request to recall files from the archive can help avoid this and is another reason why building in functionality for the system to interact directly with the storage endpoints is desirable.

## Multi-replica jobs

In addition to each FTS job supporting multiple different files for transfer, a job can also list multiple sources/destinations. An advantage here for sourcing is that only one of the possible sources needs to have the file, and that this could provide a way of sourcing a quick transfer without needing to attempt a slow transfer (as the subsequent routes are not used once the first succeeds). However, these options are mutually exclusive; a job containing multiple files cannot have more than replica (source/destination) per file. To transfer multiple files with multiple source/destinations, it would therefore be necessary to loop over either the files or the source/destinations outside of the FTS client.

Another difficulty is that settings for `bring_online` or `archive_timeout` are shared across all replicas. This means that attempting to source from both a disk and tape endpoint will not work, due to the latter requiring `bring_online` to be set.

Finally, it is technically possible to schedule a transfer with the same source and destination, however this is not helpful. If the file isn’t present, then the transfer will fail, and the next route tried. If the file is present, and overwrite is not enabled, the transfer will fail and the next will be attempted (which would also fail due to overwrite). If overwrite is enabled, then it will fail as the transfer will try to open the file for read and write at the same time. Therefore there is no scenario in which setting the destination as the source would let you implicitly check the existence of the file at the destination in order to prevent the other routes.

## XRootD CLI

To check the status of files at destinations supporting the XRootD protocol, there are command line executables installed with the `xrootd-client` package.

Movement to, from or between storage destinations can be managed with [xrdcp](https://xrootd.slac.stanford.edu/doc/man/xrdcp.1.html):
```bash
xrdcp local/file/path.txt root://hostname:1094//remote/file/path.txt
```

Other "file system" utilities are provided by [xrdfs](https://xrootd.slac.stanford.edu/doc/man/xrdfs.1.html):
```bash
xrdfs root://hostname:1094 ls /remote/file/path.txt
```
