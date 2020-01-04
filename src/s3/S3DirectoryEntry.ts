import {
  DeleteObjectsRequest,
  ObjectIdentifierList,
  PutObjectRequest
} from "aws-sdk/clients/s3";
import {
  DIR_SEPARATOR,
  DirectoryEntry,
  DirectoryEntryCallback,
  DirectoryReader,
  ErrorCallback,
  FileEntryCallback,
  Flags,
  InvalidModificationError,
  NotFoundError,
  onError,
  resolveToFullPath,
  VoidCallback
} from "kura";
import { getKey, getPath, getPrefix } from "./S3Util";
import { S3DirectoryReader } from "./S3DirectoryReader";
import { S3Entry } from "./S3Entry";
import { S3FileEntry } from "./S3FileEntry";

export class S3DirectoryEntry extends S3Entry implements DirectoryEntry {
  public static CheckDirectoryExistance = false;

  isFile = false;
  isDirectory = true;

  createReader(): DirectoryReader {
    return new S3DirectoryReader(this);
  }

  doGetFile(
    key: string,
    successCallback: FileEntryCallback,
    errorCallback?: ErrorCallback
  ) {
    const filesystem = this.filesystem;
    filesystem.s3.headObject(
      { Bucket: filesystem.bucket, Key: key },
      (err, data) => {
        if (err) {
          if (err.statusCode === 404) {
            onError(
              new NotFoundError(this.filesystem.name, getPath(key)),
              errorCallback
            );
          } else {
            onError(err, errorCallback);
          }
        } else {
          const name = key.split(DIR_SEPARATOR).pop();
          successCallback(
            new S3FileEntry({
              filesystem: this.filesystem,
              name: name,
              fullPath: DIR_SEPARATOR + key,
              lastModified: data.LastModified.getTime(),
              size: data.ContentLength
            })
          );
        }
      }
    );
  }

  doCrateFile(
    key: string,
    successCallback: FileEntryCallback,
    errorCallback?: ErrorCallback
  ) {
    const filesystem = this.filesystem;
    const request: PutObjectRequest = {
      Bucket: filesystem.bucket,
      Key: key,
      Body: "",
      ContentType: "application/octet-stream"
    };
    filesystem.s3.putObject(request, err => {
      if (err) {
        errorCallback(err);
        return;
      }
      this.doGetFile(key, successCallback, errorCallback);
    });
  }

  getFile(
    path: string,
    options?: Flags,
    successCallback?: FileEntryCallback,
    errorCallback?: ErrorCallback
  ): void {
    if (!options) {
      options = {};
    }
    if (!successCallback) {
      successCallback = () => {};
    }

    path = resolveToFullPath(this.fullPath, path);
    const key = getKey(path);
    this.doGetFile(
      key,
      entry => {
        if (entry.isDirectory) {
          const path = getPath(key);
          onError(
            new InvalidModificationError(
              this.filesystem.name,
              path,
              `${path} is not a file`
            ),
            errorCallback
          );
          return;
        }
        if (options.create && options.exclusive) {
          onError(
            new InvalidModificationError(
              this.filesystem.name,
              path,
              `${path} already exists`
            ),
            errorCallback
          );
          return;
        }
        successCallback(entry);
      },
      err => {
        if (err instanceof NotFoundError && options.create) {
          this.doCrateFile(key, successCallback, errorCallback);
        } else {
          onError(err, errorCallback);
        }
      }
    );
  }

  async hasChild(path: string) {
    const filesystem = this.filesystem;
    const prefix = getPrefix(path);
    const param: AWS.S3.ListObjectsV2Request = {
      Bucket: filesystem.bucket,
      Prefix: prefix,
      Delimiter: DIR_SEPARATOR,
      MaxKeys: 1
    };

    const data = await filesystem.s3.listObjectsV2(param).promise();
    return 0 < data.CommonPrefixes.length || 0 < data.Contents.length;
  }

  doGetDirectory(
    path: string,
    options: Flags,
    successCallback: DirectoryEntryCallback,
    errorCallback?: ErrorCallback
  ) {
    const filesystem = this.filesystem;
    const name = path.split(DIR_SEPARATOR).pop();
    if (!S3DirectoryEntry.CheckDirectoryExistance || options.create) {
      successCallback(
        new S3DirectoryEntry({
          filesystem: filesystem,
          name: name,
          fullPath: path,
          lastModified: null,
          size: null,
          hash: null
        })
      );
    } else {
      this.hasChild(path)
        .then(result => {
          if (result) {
            successCallback(
              new S3DirectoryEntry({
                filesystem: filesystem,
                name: name,
                fullPath: path,
                lastModified: null,
                size: null,
                hash: null
              })
            );
            return;
          }
          errorCallback(new NotFoundError(this.filesystem.name, path));
        })
        .catch(err => {
          onError(err, errorCallback);
        });
    }
  }

  getDirectory(
    path: string,
    options?: Flags,
    successCallback?: DirectoryEntryCallback,
    errorCallback?: ErrorCallback
  ): void {
    if (!successCallback) {
      successCallback = () => {};
    }

    path = resolveToFullPath(this.fullPath, path);
    const key = getKey(path);
    this.doGetFile(
      key,
      () => {
        onError(
          new InvalidModificationError(
            this.filesystem.name,
            path,
            `${path} is not a directory`
          ),
          errorCallback
        );
      },
      err => {
        if (err instanceof NotFoundError) {
          if (!options) {
            options = {};
          }
          this.doGetDirectory(path, options, successCallback, errorCallback);
        } else {
          onError(err, errorCallback);
        }
      }
    );
  }

  remove(successCallback: VoidCallback, errorCallback?: ErrorCallback): void {
    const key = getKey(this.fullPath);
    this.doGetFile(
      key,
      () => {
        onError(
          new InvalidModificationError(
            this.filesystem.name,
            this.fullPath,
            `${this.fullPath} is not a directory`
          ),
          errorCallback
        );
      },
      err => {
        if (err instanceof NotFoundError) {
          this.hasChild(this.fullPath)
            .then(result => {
              if (result) {
                onError(
                  new InvalidModificationError(
                    this.filesystem.name,
                    this.fullPath,
                    `${this.fullPath} is not empty`
                  ),
                  errorCallback
                );
              } else {
                successCallback();
              }
            })
            .catch(err => {
              onError(err, errorCallback);
            });
        } else {
          onError(err, errorCallback);
        }
      }
    );
  }

  removeRecursively(
    successCallback: VoidCallback,
    errorCallback?: ErrorCallback
  ): void {
    const prefix = getPrefix(this.fullPath);
    const filesystem = this.filesystem;
    const s3 = filesystem.s3;
    s3.listObjectsV2(
      { Bucket: filesystem.bucket, Prefix: prefix },
      (err, listData) => {
        if (err) {
          onError(err, errorCallback);
          return;
        }

        if (listData.Contents.length === 0) {
          successCallback();
        }

        const objects: ObjectIdentifierList = [];
        const params: DeleteObjectsRequest = {
          Bucket: filesystem.bucket,
          Delete: { Objects: objects }
        };

        listData.Contents.forEach(function(content) {
          objects.push({ Key: content.Key });
        });

        s3.deleteObjects(params, err => {
          if (err) {
            onError(err, errorCallback);
            return;
          }
          if (listData.Contents.length === 1000) {
            this.removeRecursively(successCallback, errorCallback);
            return;
          }
          successCallback();
        });
      }
    );
  }
}
