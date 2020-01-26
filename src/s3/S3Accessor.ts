import {
  AbstractAccessor,
  DIR_SEPARATOR,
  FileSystem,
  FileSystemObject
} from "kura";
import { DeleteObjectRequest, ListObjectsV2Request } from "aws-sdk/clients/s3";
import { getKey, getPrefix } from "./S3Util";
import { S3FileSystem } from "./S3FileSystem";
import S3 = require("aws-sdk/clients/s3");

export class S3Accessor extends AbstractAccessor {
  filesystem: FileSystem;
  name: string;
  s3: S3;

  constructor(
    options: S3.ClientConfiguration,
    bucket: string,
    useIndex: boolean
  ) {
    super(useIndex);
    this.s3 = new S3(options);
    this.filesystem = new S3FileSystem(this);
    this.name = bucket;
  }

  async getContent(fullPath: string) {
    try {
      const data = await this.s3
        .getObject({ Bucket: this.name, Key: getKey(fullPath) })
        .promise();
      const content = data.Body.valueOf();
      let blob: Blob;
      if (content instanceof Buffer) {
        // Node
        const buffer = content as Buffer;
        blob = new Blob([buffer]);
      } else {
        // Browser
        blob = content as Blob;
      }
      return blob;
    } catch (err) {
      if (err.statusCode === 404) {
        return null;
      }
      throw err;
    }
  }

  async getObject(fullPath: string): Promise<FileSystemObject> {
    const key = getKey(fullPath);
    try {
      const data = await this.s3
        .headObject({
          Bucket: this.name,
          Key: key
        })
        .promise();
      const name = key.split(DIR_SEPARATOR).pop();
      return {
        name: name,
        fullPath: DIR_SEPARATOR + key,
        lastModified: data.LastModified.getTime(),
        size: data.ContentLength
      };
    } catch (err) {
      if (err.statusCode === 404) {
        return null;
      }
      throw err;
    }
  }

  protected async doDelete(fullPath: string, isFile: boolean) {
    if (!isFile) {
      return;
    }
    const key = getKey(fullPath);
    const params: DeleteObjectRequest = {
      Bucket: this.name,
      Key: key
    };
    await this.s3.deleteObject(params).promise();
  }

  protected async doGetObjects(fullPath: string) {
    const prefix = getPrefix(fullPath);
    const params: ListObjectsV2Request = {
      Bucket: this.name,
      Delimiter: DIR_SEPARATOR,
      Prefix: prefix,
      ContinuationToken: null
    };
    const objects: FileSystemObject[] = [];
    await this.doGetObjectsFromS3(params, fullPath, objects);
    return objects;
  }

  protected async doGetObjectsFromS3(
    params: ListObjectsV2Request,
    fullPath: string,
    objects: FileSystemObject[]
  ) {
    const data = await this.s3.listObjectsV2(params).promise();
    for (const content of data.CommonPrefixes) {
      const parts = content.Prefix.split(DIR_SEPARATOR);
      const name = parts[parts.length - 2];
      objects.push({
        name: name,
        fullPath: (fullPath === "/" ? "" : fullPath) + DIR_SEPARATOR + name,
        lastModified: null,
        size: null
      });
    }
    for (const content of data.Contents) {
      const parts = content.Key.split(DIR_SEPARATOR);
      const name = parts[parts.length - 1];
      objects.push({
        name: name,
        fullPath: (fullPath === "/" ? "" : fullPath) + DIR_SEPARATOR + name,
        lastModified: content.LastModified.getTime(),
        size: content.Size
      });
    }

    if (data.IsTruncated) {
      params.ContinuationToken = data.NextContinuationToken;
      await this.doGetObjectsFromS3(params, fullPath, objects);
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
    await this.s3
      .putObject({
        Bucket: this.name,
        Key: getKey(fullPath),
        Body: body
      })
      .promise();
  }

  protected async doPutObject(obj: FileSystemObject) {
    const key = getKey(obj.fullPath);
    const request: S3.PutObjectRequest = {
      Bucket: this.name,
      Key: key,
      Body: "",
      ContentType: "application/octet-stream"
    };
    await this.s3.putObject(request).promise();
  }
}
