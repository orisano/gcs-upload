# gcs-upload

gcs-upload is a command-line tool written in Go for uploading files to Google Cloud Storage (GCS). It provides a convenient alternative to the `gsutil -m cp -r` command.

## Installation

### Binary Installation

You can download the binary for gcs-upload from the [Releases](https://github.com/orisano/gcs-upload/releases) page. Choose the appropriate binary for your operating system and architecture, and download it.

Alternatively, you can use the `go install` command to install gcs-upload directly:

```shell
go install github.com/orisano/gcs-upload@latest
```

### Container Image

A container image for gcs-upload is also available on <u>ghcr.io</u>. This image is intended for use with Cloud Build.

To use the container image, you can pull it using Docker:

```shell
docker pull ghcr.io/orisano/gcs-upload:<tag>
```

Replace `<tag>` with the desired version or tag of the image.

## Usage

To upload files to Google Cloud Storage (GCS) using gcs-upload, use the following command:

```shell
gcs-upload [options] <dest>
```

The `<dest>` argument specifies the target directory on GCS where the files will be uploaded. It should be in the form of a GCS path starting with `gs://`.

Options
- `-buf value`: Set the copy buffer size (default: 512k).
- `-chunk value`: Set the upload chunk size (default: 16m).
- `-d string`: Set the local directory containing the files to be uploaded.
- `-gc int`: Set the garbage collection (GC) interval.
- `-l string: Upload files specified in the target list-file.
- `-n int`: Set the number of goroutines for uploading (default: 24).
- `-shuffle`: Shuffle the upload order.
- `-v`: Show verbose output.

Note: Square brackets in the command indicate optional parameters.

### Examples

Upload files from a specific local directory to a target directory on GCS:

```shell
gcs-upload -d <local-dir> gs://<dest>
```

## License
This project is licensed under the MIT License. See the LICENSE file for details.

