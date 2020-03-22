import { AWSError } from "aws-sdk";
import { DeleteObjectRequest, ListObjectsV2Request } from "aws-sdk/clients/s3";
import {
  AbstractAccessor,
  blobToArrayBuffer,
  DIR_SEPARATOR,
  FileSystem,
  FileSystemObject,
  InvalidModificationError,
  normalizePath,
  NotFoundError,
  NotReadableError,
  xhrGet,
  xhrPut
} from "kura";
import { S3FileSystem } from "./S3FileSystem";
import { S3FileSystemOptions } from "./S3FileSystemOption";
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
    private s3Options?: S3FileSystemOptions
  ) {
    super(s3Options);
    this.s3 = new S3(config);
    this.filesystem = new S3FileSystem(this);
    this.name = bucket + rootDir;
  }

  async doDelete(fullPath: string, isFile: boolean) {
    if (!isFile) {
      return;
    }
    const path = normalizePath(this.rootDir + DIR_SEPARATOR + fullPath);
    const key = getKey(path);
    const params: DeleteObjectRequest = {
      Bucket: this.bucket,
      Key: key
    };
    try {
      await this.s3.deleteObject(params).promise();
    } catch (err) {
      if ((err as AWSError).statusCode === 404) {
        throw new NotFoundError(this.name, fullPath, err);
      }
      throw new InvalidModificationError(this.name, fullPath, err);
    }
  }

  async doGetContent(fullPath: string) {
    if (this.s3Options.methodOfDoGetContent === "xhr") {
      return await this.doGetContentUsingXHR(fullPath);
    } else {
      return await this.doGetContentUsingGetObject(fullPath);
    }
  }

  async doGetObject(fullPath: string): Promise<FileSystemObject> {
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
      if ((err as AWSError).statusCode === 404) {
        throw new NotFoundError(this.name, fullPath, err);
      }
      throw new NotReadableError(this.name, fullPath, err);
    }
  }

  async doGetObjects(dirPath: string) {
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

  async doPutContent(fullPath: string, content: Blob) {
    const method = this.s3Options.methodOfDoPutContent;

    if (method === "xhr") {
      await this.doPutContentUsingXHR(fullPath, content);
    } else if (method === "upload") {
      await this.doPutContentUsingUpload(fullPath, content);
    } else if (method === "uploadPart") {
      await this.doPutContentUsingUploadPart(fullPath, content);
    } else {
      await this.doPutContentUsingPutObject(fullPath, content);
    }
  }

  async doPutObject(obj: FileSystemObject) {}

  private async doGetContentUsingGetObject(fullPath: string) {
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
      if ((err as AWSError).statusCode === 404) {
        throw new NotFoundError(this.name, fullPath, err);
      }
      throw new NotReadableError(this.name, fullPath, err);
    }
  }

  private async doGetContentUsingXHR(fullPath: string) {
    try {
      const path = normalizePath(this.rootDir + DIR_SEPARATOR + fullPath);
      const key = getKey(path);

      // check existance
      await this.s3
        .headObject({
          Bucket: this.bucket,
          Key: key
        })
        .promise();

      const url = await this.s3.getSignedUrlPromise("getObject", {
        Bucket: this.bucket,
        Key: key
      });
      return await xhrGet(url, this.name, fullPath);
    } catch (err) {
      if ((err as AWSError).statusCode === 404) {
        throw new NotFoundError(this.name, fullPath, err);
      }
      throw new NotReadableError(this.name, fullPath, err);
    }
  }

  private async doGetObjectsFromS3(
    params: ListObjectsV2Request,
    dirPath: string,
    path: string,
    objects: FileSystemObject[]
  ) {
    try {
      var data = await this.s3.listObjectsV2(params).promise();
    } catch (err) {
      if ((err as AWSError).statusCode === 404) {
        throw new NotFoundError(this.name, dirPath, err);
      }
      throw new NotReadableError(this.name, dirPath, err);
    }
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

  private async doPutContentUsingPutObject(fullPath: string, content: Blob) {
    const body = await this.getBody(content);
    const path = normalizePath(this.rootDir + DIR_SEPARATOR + fullPath);
    const key = getKey(path);
    const url = this.s3.getSignedUrl("putObject", {
      Bucket: this.bucket,
      Key: key
    });
    try {
      await this.s3
        .putObject({
          Bucket: this.bucket,
          Key: key,
          Body: body
        })
        .promise();
    } catch (err) {
      throw new InvalidModificationError(this.name, fullPath, err);
    }
  }

  private async doPutContentUsingUpload(fullPath: string, content: Blob) {
    const body = await this.getBody(content);
    const path = normalizePath(this.rootDir + DIR_SEPARATOR + fullPath);
    const key = getKey(path);
    await this.s3
      .upload({
        Bucket: this.bucket,
        Key: key,
        Body: body
      })
      .promise();
  }

  private async doPutContentUsingUploadPart(fullPath: string, content: Blob) {
    const path = normalizePath(this.rootDir + DIR_SEPARATOR + fullPath);
    const key = getKey(path);

    const allSize = content.size;
    const partSize = 1024 * 1024; // 1MB chunk
    const multipartMap: S3.CompletedMultipartUpload = {
      Parts: []
    };

    const createReq: S3.CreateMultipartUploadRequest = {
      Bucket: this.bucket,
      Key: key
    };
    const multiPartUpload = await this.s3
      .createMultipartUpload(createReq)
      .promise();
    const uploadId = multiPartUpload.UploadId;

    let partNum = 0;
    const { ContentType, ...otherParams } = createReq;
    for (let rangeStart = 0; rangeStart < allSize; rangeStart += partSize) {
      partNum++;
      const end = Math.min(rangeStart + partSize, allSize);
      const sliced = content.slice(rangeStart, end);
      const chunk = blobToArrayBuffer(sliced);

      const partParams: S3.UploadPartRequest = {
        Body: chunk,
        PartNumber: partNum,
        UploadId: uploadId,
        ...otherParams
      };
      const uploadPart = await this.s3.uploadPart(partParams).promise();

      multipartMap.Parts[partNum - 1] = {
        ETag: uploadPart.ETag,
        PartNumber: partNum
      };
    }

    const completeReq: S3.CompleteMultipartUploadRequest = {
      ...otherParams,
      MultipartUpload: multipartMap,
      UploadId: uploadId
    };

    await this.s3.completeMultipartUpload(completeReq).promise();
  }

  private async doPutContentUsingXHR(fullPath: string, content: Blob) {
    try {
      const path = normalizePath(this.rootDir + DIR_SEPARATOR + fullPath);
      const key = getKey(path);

      const url = await this.s3.getSignedUrlPromise("putObject", {
        Bucket: this.bucket,
        Key: key
      });
      await xhrPut(url, content, this.name, fullPath);
    } catch (err) {
      throw new InvalidModificationError(this.name, fullPath, err);
    }
  }

  private async getBody(content: Blob) {
    if (typeof process === "object") {
      // Node
      const buffer = await blobToArrayBuffer(content);
      const view = new Uint8Array(buffer);
      return Buffer.from(view);
    } else {
      // Web
      return content;
    }
  }
}
