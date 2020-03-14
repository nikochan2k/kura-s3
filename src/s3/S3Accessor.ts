import { DeleteObjectRequest, ListObjectsV2Request } from "aws-sdk/clients/s3";
import {
  AbstractAccessor,
  DIR_SEPARATOR,
  FileSystem,
  FileSystemObject,
  normalizePath
} from "kura";
import { FileSystemOptions } from "kura/lib/FileSystemOptions";
import { S3FileSystem } from "./S3FileSystem";
import { getKey, getPrefix } from "./S3Util";
import S3 = require("aws-sdk/clients/s3");

export class S3Accessor extends AbstractAccessor {
  filesystem: FileSystem;
  name: string;
  s3: S3;

  constructor(
    config: S3.ClientConfiguration,
    private bucket: string,
    private rootDir: string,
    options?: FileSystemOptions
  ) {
    super(options);
    this.s3 = new S3(config);
    this.filesystem = new S3FileSystem(this);
    this.name = bucket + rootDir;
  }

  async resetObject(fullPath: string, size?: number) {
    const path = normalizePath(this.rootDir + DIR_SEPARATOR + fullPath);
    const obj = await this.doGetObject(path);
    if (!obj) {
      return null;
    }
    await this.putObject(obj);
    return obj;
  }

  protected async doDelete(fullPath: string, isFile: boolean) {
    if (!isFile) {
      return;
    }
    const path = normalizePath(this.rootDir + DIR_SEPARATOR + fullPath);
    const key = getKey(path);
    const params: DeleteObjectRequest = {
      Bucket: this.bucket,
      Key: key
    };
    await this.s3.deleteObject(params).promise();
  }

  protected async doGetContent(fullPath: string) {
    try {
      const path = normalizePath(this.rootDir + DIR_SEPARATOR + fullPath);
      const key = getKey(path);
      const data = await this.s3
        .getObject({ Bucket: this.bucket, Key: key })
        .promise();
      const content = data.Body.valueOf();
      let blob: Blob;
      if (content instanceof Blob) {
        // Browser
        blob = content as Blob;
      } else {
        // Node
        blob = new Blob([content as any]);
      }
      return blob;
    } catch (err) {
      if (err.statusCode !== 404) {
        console.error(err);
      }
      return null;
    }
  }

  protected async doGetObject(fullPath: string): Promise<FileSystemObject> {
    const path = normalizePath(this.rootDir + DIR_SEPARATOR + fullPath);
    const key = getKey(path);
    try {
      const data = await this.s3
        .headObject({
          Bucket: this.bucket,
          Key: key
        })
        .promise();
      const name = key.split(DIR_SEPARATOR).pop();
      return {
        name: name,
        fullPath: fullPath,
        lastModified: data.LastModified.getTime(),
        size: data.ContentLength
      };
    } catch (err) {
      if (err.statusCode !== 404) {
        console.error(err);
      }
      return null;
    }
  }

  protected async doGetObjects(dirPath: string) {
    const path = normalizePath(this.rootDir + DIR_SEPARATOR + dirPath);
    const prefix = getPrefix(path);
    const params: ListObjectsV2Request = {
      Bucket: this.bucket,
      Delimiter: DIR_SEPARATOR,
      Prefix: prefix,
      ContinuationToken: null
    };
    const objects: FileSystemObject[] = [];
    await this.doGetObjectsFromS3(params, dirPath, path, objects);
    return objects;
  }

  protected async doGetObjectsFromS3(
    params: ListObjectsV2Request,
    dirPath: string,
    path: string,
    objects: FileSystemObject[]
  ) {
    const data = await this.s3.listObjectsV2(params).promise();
    for (const content of data.CommonPrefixes) {
      const parts = content.Prefix.split(DIR_SEPARATOR);
      const name = parts[parts.length - 2];
      const fullPath = normalizePath(dirPath + DIR_SEPARATOR + name);
      objects.push({
        name: name,
        fullPath: fullPath,
        lastModified: null,
        size: null
      });
    }
    for (const content of data.Contents) {
      const parts = content.Key.split(DIR_SEPARATOR);
      const name = parts[parts.length - 1];
      const fullPath = normalizePath(dirPath + DIR_SEPARATOR + name);
      objects.push({
        name: name,
        fullPath: fullPath,
        lastModified: content.LastModified.getTime(),
        size: content.Size
      });
    }

    if (data.IsTruncated) {
      params.ContinuationToken = data.NextContinuationToken;
      await this.doGetObjectsFromS3(params, dirPath, path, objects);
    }
  }

  protected async doPutContent(fullPath: string, content: Blob) {
    let body: any;
    if (typeof process === "object") {
      // Node
      const sendData = await new Promise<Uint8Array>(resolve => {
        const fileReader = new FileReader();
        fileReader.onloadend = event => {
          const data = event.target.result as ArrayBuffer;
          const byte = new Uint8Array(data);
          resolve(byte);
        };
        fileReader.readAsArrayBuffer(content);
      });
      body = Buffer.from(sendData);
    } else {
      // Browser
      body = content;
    }
    const path = normalizePath(this.rootDir + DIR_SEPARATOR + fullPath);
    const key = getKey(path);
    await this.s3
      .putObject({
        Bucket: this.bucket,
        Key: key,
        Body: body
      })
      .promise();
  }

  protected async doPutObject(obj: FileSystemObject) {}
}
