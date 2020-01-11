import {
  AbstractDirectoryEntry,
  DIR_SEPARATOR,
  DirectoryEntryCallback,
  DirectoryReader,
  ErrorCallback,
  FileEntryCallback,
  FileSystemParams,
  Flags,
  InvalidModificationError,
  NotFoundError,
  onError,
  resolveToFullPath,
  VoidCallback
} from "kura";
import {
  DeleteObjectRequest,
  DeleteObjectsRequest,
  ObjectIdentifierList,
  PutObjectRequest
} from "aws-sdk/clients/s3";
import { getKey, getPath, getPrefix } from "./S3Util";
import { S3DirectoryReader } from "./S3DirectoryReader";
import { S3EntrySupport } from "./S3EntrySupport";
import { S3FileEntry } from "./S3FileEntry";
import { S3FileSystem } from "./S3FileSystem";

export class S3DirectoryEntry extends AbstractDirectoryEntry<S3FileSystem> {
  public static CheckDirectoryExistance = false;

  constructor(params: FileSystemParams<S3FileSystem>) {
    super(params, new S3EntrySupport(params));
  }

  createReader(): DirectoryReader {
    return new S3DirectoryReader(this);
  }

  async delete(): Promise<void> {
    const key = getKey(this.fullPath);
    try {
      await this.doGetFile(key);
      throw new InvalidModificationError(
        this.filesystem.name,
        this.fullPath,
        `${this.fullPath} is not a directory`
      );
    } catch (err) {
      if (err instanceof NotFoundError) {
        const result = await this.hasChild();
        if (result) {
          throw new InvalidModificationError(
            this.filesystem.name,
            this.fullPath,
            `${this.fullPath} is not empty`
          );
        } else {
          const params: DeleteObjectRequest = {
            Bucket: this.filesystem.bucket,
            Key: key
          };
          await this.filesystem.s3.deleteObject(params).promise();
        }
      }
    }
  }

  async doCrateFile(key: string) {
    const filesystem = this.filesystem;
    const request: PutObjectRequest = {
      Bucket: filesystem.bucket,
      Key: key,
      Body: "",
      ContentType: "application/octet-stream"
    };
    await filesystem.s3.putObject(request).promise();
    return await this.doGetFile(key);
  }

  async doGetDirectory(path: string, options: Flags) {
    const filesystem = this.filesystem;
    const name = path.split(DIR_SEPARATOR).pop();
    if (!S3DirectoryEntry.CheckDirectoryExistance || options.create) {
      return new S3DirectoryEntry({
        filesystem: filesystem,
        name: name,
        fullPath: path
      });
    } else {
      const result = await this.doHasChild(path);
      if (!result) {
        new NotFoundError(this.filesystem.name, path);
      }
      return new S3DirectoryEntry({
        filesystem: filesystem,
        name: name,
        fullPath: path
      });
    }
  }

  async doGetFile(key: string) {
    const filesystem = this.filesystem;
    try {
      const data = await filesystem.s3
        .headObject({
          Bucket: filesystem.bucket,
          Key: key
        })
        .promise();
      const name = key.split(DIR_SEPARATOR).pop();
      return new S3FileEntry({
        filesystem: this.filesystem,
        name: name,
        fullPath: DIR_SEPARATOR + key,
        lastModified: data.LastModified.getTime(),
        size: data.ContentLength
      });
    } catch (err) {
      if (err.statusCode === 404) {
        throw new NotFoundError(this.filesystem.name, getPath(key));
      }
      throw err;
    }
  }

  async doHasChild(path: string) {
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
    this.doGetFile(key)
      .then(() => {
        onError(
          new InvalidModificationError(
            this.filesystem.name,
            path,
            `${path} is not a directory`
          ),
          errorCallback
        );
      })
      .catch(async err => {
        if (err instanceof NotFoundError) {
          if (!options) {
            options = {};
          }
          this.doGetDirectory(path, options)
            .then(entry => {
              successCallback(entry);
            })
            .catch(err => {
              onError(err, errorCallback);
            });
        } else {
          onError(err, errorCallback);
        }
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
    this.doGetFile(key)
      .then(entry => {
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
      })
      .catch(async err => {
        if (err instanceof NotFoundError && options.create) {
          this.doCrateFile(key)
            .then(entry => {
              successCallback(entry);
            })
            .catch(err => {
              onError(err, errorCallback);
            });
        } else {
          onError(err, errorCallback);
        }
      });
  }

  async hasChild() {
    return this.doHasChild(this.fullPath);
  }

  registerObject(
    path: string,
    isFile: boolean
  ): Promise<import("kura").FileSystemObject> {
    throw new Error("Method not implemented.");
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
