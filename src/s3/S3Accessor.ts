import { AWSError, config } from "aws-sdk";
import { ClientConfiguration } from "aws-sdk/clients/acm";
import S3, {
  CompletedMultipartUpload,
  CompleteMultipartUploadRequest,
  CreateMultipartUploadRequest,
  DeleteObjectRequest,
  ListObjectsV2Request,
  UploadPartRequest,
} from "aws-sdk/clients/s3";
import {
  AbstractAccessor,
  AbstractFileError,
  DIR_SEPARATOR,
  FileSystem,
  FileSystemObject,
  InvalidModificationError,
  normalizePath,
  NotFoundError,
  NotReadableError,
  textToArrayBuffer,
  toArrayBuffer,
  toBlob,
  XHR,
} from "kura";
import { S3FileSystem } from "./S3FileSystem";
import { S3FileSystemOptions } from "./S3FileSystemOption";
import { getKey, getPrefix } from "./S3Util";

export class S3Accessor extends AbstractAccessor {
  // #region Properties (3)

  public filesystem: FileSystem;
  public name: string;
  public s3: S3;

  // #endregion Properties (3)

  // #region Constructors (1)

  constructor(
    config: ClientConfiguration,
    private bucket: string,
    private rootDir: string,
    private s3Options?: S3FileSystemOptions
  ) {
    super(s3Options);
    if (!config.httpOptions) {
      config.httpOptions = {};
    }
    if (config.httpOptions.timeout == null) {
      config.httpOptions.timeout = 30000;
      config.httpOptions.connectTimeout = 2000;
    }
    config.signatureVersion = "v4";
    this.s3 = new S3(config);
    this.filesystem = new S3FileSystem(this);
    if (!this.rootDir.startsWith(DIR_SEPARATOR)) {
      this.rootDir = DIR_SEPARATOR + this.rootDir;
    }
    this.name = this.bucket + this.rootDir;
  }

  // #endregion Constructors (1)

  // #region Public Methods (5)

  public async doDelete(fullPath: string, isFile: boolean) {
    if (!isFile) {
      return;
    }
    const key = this.getKey(fullPath);
    const params: DeleteObjectRequest = {
      Bucket: this.bucket,
      Key: key,
    };
    try {
      await this.s3.deleteObject(params).promise();
    } catch (err) {
      if (err instanceof AbstractFileError) {
        throw err;
      }
      if ((err as AWSError).statusCode === 404) {
        throw new NotFoundError(this.name, fullPath, err);
      }
      throw new InvalidModificationError(this.name, fullPath, err);
    }
  }

  public async doGetObject(fullPath: string): Promise<FileSystemObject> {
    const key = this.getKey(fullPath);
    try {
      const data = await this.s3
        .headObject({
          Bucket: this.bucket,
          Key: key,
        })
        .promise();
      const name = key.split(DIR_SEPARATOR).pop();
      const url = await this.s3.getSignedUrlPromise("getObject", {
        Bucket: this.bucket,
        Key: key,
        Expires: 60 * 60 * 24 * 7,
      });
      return {
        name,
        fullPath: fullPath,
        lastModified: data.LastModified.getTime(),
        size: data.ContentLength,
        url,
      };
    } catch (err) {
      if (err instanceof AbstractFileError) {
        throw err;
      }
      if ((err as AWSError).statusCode === 404) {
        throw new NotFoundError(this.name, fullPath, err);
      }
      throw new NotReadableError(this.name, fullPath, err);
    }
  }

  public async doGetObjects(dirPath: string) {
    const path = normalizePath(this.rootDir + DIR_SEPARATOR + dirPath);
    const prefix = getPrefix(path);
    const params: ListObjectsV2Request = {
      Bucket: this.bucket,
      Delimiter: DIR_SEPARATOR,
      Prefix: prefix,
      ContinuationToken: null,
    };

    const objects: FileSystemObject[] = [];
    await this.doReadObjectsFromS3(params, dirPath, path, objects);
    return objects;
  }

  public async doMakeDirectory(obj: FileSystemObject) {
    // NOOP
  }

  public async doReadContent(
    fullPath: string
  ): Promise<Blob | Uint8Array | ArrayBuffer | string> {
    if (this.s3Options.methodOfDoGetContent === "xhr") {
      return await this.doReadContentUsingXHR(fullPath, "arraybuffer");
    } else {
      return await this.doReadContentUsingGetObject(fullPath);
    }
  }

  // #endregion Public Methods (5)

  // #region Protected Methods (3)

  protected doWriteArrayBuffer(
    fullPath: string,
    buffer: ArrayBuffer
  ): Promise<void> {
    return this.doReadContentToS3(fullPath, buffer);
  }

  protected async doWriteBase64(
    fullPath: string,
    base64: string
  ): Promise<void> {
    const buffer = await toArrayBuffer(base64);
    return this.doReadContentToS3(fullPath, buffer);
  }

  protected doWriteBlob(fullPath: string, blob: Blob): Promise<void> {
    return this.doReadContentToS3(fullPath, blob);
  }

  // #endregion Protected Methods (3)

  // #region Private Methods (11)

  private async doReadContentToS3(
    fullPath: string,
    content: Blob | ArrayBuffer
  ) {
    const method = this.s3Options.methodOfDoPutContent;

    if (method === "xhr") {
      await this.doWriteContentUsingXHR(fullPath, content);
    } else if (method === "upload") {
      await this.doWriteContentUsingUpload(fullPath, content);
    } else if (method === "uploadPart") {
      if (typeof content === "string") {
        const blob = new Blob([content]);
        await this.doWriteContentUsingUploadPart(fullPath, blob);
      } else {
        await this.doWriteContentUsingUploadPart(fullPath, content);
      }
    } else {
      await this.doWriteContentUsingPutObject(fullPath, content);
    }
  }

  private async doReadContentUsingGetObject(fullPath: string) {
    try {
      const key = this.getKey(fullPath);
      const data = await this.s3
        .getObject({
          Bucket: this.bucket,
          Key: key,
          ResponseCacheControl: "no-cache",
        })
        .promise();
      const body = data.Body;
      return this.fromBody(body);
    } catch (err) {
      if (err instanceof AbstractFileError) {
        throw err;
      }
      if ((err as AWSError).statusCode === 404) {
        throw new NotFoundError(this.name, fullPath, err);
      }
      throw new NotReadableError(this.name, fullPath, err);
    }
  }

  private async doReadContentUsingXHR(
    fullPath: string,
    responseType: XMLHttpRequestResponseType
  ) {
    try {
      const obj = await this.doGetObject(fullPath);
      const xhr = new XHR(this.name, fullPath, {
        timeout: config.httpOptions.timeout,
      });
      return xhr.get(obj.url, responseType);
    } catch (err) {
      if (err instanceof AbstractFileError) {
        throw err;
      }
      if ((err as AWSError).statusCode === 404) {
        throw new NotFoundError(this.name, fullPath, err);
      }
      throw new NotReadableError(this.name, fullPath, err);
    }
  }

  private async doReadObjectsFromS3(
    params: ListObjectsV2Request,
    dirPath: string,
    path: string,
    objects: FileSystemObject[]
  ) {
    try {
      var data = await this.s3.listObjectsV2(params).promise();
    } catch (err) {
      if (err instanceof AbstractFileError) {
        throw err;
      }
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
        size: null,
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
        size: content.Size,
      });
    }

    if (data.IsTruncated) {
      params.ContinuationToken = data.NextContinuationToken;
      await this.doReadObjectsFromS3(params, dirPath, path, objects);
    }
  }

  private async doWriteContentUsingPutObject(
    fullPath: string,
    content: Blob | ArrayBuffer
  ) {
    const body = await this.toBody(content);
    const key = this.getKey(fullPath);
    try {
      await this.s3
        .putObject({
          Bucket: this.bucket,
          Key: key,
          Body: body,
        })
        .promise();
    } catch (err) {
      if (err instanceof AbstractFileError) {
        throw err;
      }
      throw new InvalidModificationError(this.name, fullPath, err);
    }
  }

  private async doWriteContentUsingUpload(
    fullPath: string,
    content: Blob | ArrayBuffer
  ) {
    const body = await this.toBody(content);
    const key = this.getKey(fullPath);
    await this.s3
      .upload({
        Bucket: this.bucket,
        Key: key,
        Body: body,
      })
      .promise();
  }

  private async doWriteContentUsingUploadPart(
    fullPath: string,
    content: Blob | ArrayBuffer
  ) {
    const key = this.getKey(fullPath);

    const buffer = await toArrayBuffer(content); // TODO
    const view = new Uint8Array(buffer);
    const allSize = view.byteLength;
    const partSize = 1024 * 1024; // 1MB chunk
    const multipartMap: CompletedMultipartUpload = {
      Parts: [],
    };

    const createReq: CreateMultipartUploadRequest = {
      Bucket: this.bucket,
      Key: key,
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
      const chunk = view.slice(rangeStart, end);
      const partParams: UploadPartRequest = {
        Body: chunk,
        PartNumber: partNum,
        UploadId: uploadId,
        ...otherParams,
      };
      const uploadPart = await this.s3.uploadPart(partParams).promise();

      multipartMap.Parts[partNum - 1] = {
        ETag: uploadPart.ETag,
        PartNumber: partNum,
      };
    }

    const completeReq: CompleteMultipartUploadRequest = {
      ...otherParams,
      MultipartUpload: multipartMap,
      UploadId: uploadId,
    };

    await this.s3.completeMultipartUpload(completeReq).promise();
  }

  private async doWriteContentUsingXHR(
    fullPath: string,
    content: Blob | ArrayBuffer
  ) {
    try {
      const key = this.getKey(fullPath);

      const url = await this.s3.getSignedUrlPromise("putObject", {
        Bucket: this.bucket,
        Key: key,
        Expires: 60 * 60 * 24 * 7,
      });
      const xhr = new XHR(this.name, fullPath, {
        timeout: config.httpOptions.timeout,
      });
      if (content instanceof ArrayBuffer) {
        const view = new Uint8Array(content);
        await xhr.put(url, view);
      } else {
        await xhr.put(url, content);
      }
    } catch (err) {
      if (err instanceof AbstractFileError) {
        throw err;
      }
      throw new InvalidModificationError(this.name, fullPath, err);
    }
  }

  private async fromBody(body: any) {
    let content: Blob | ArrayBuffer;
    if (typeof process === "object" && body instanceof Buffer) {
      const view = new Uint8Array(body).buffer;
      content = await toArrayBuffer(view);
    } else if (body instanceof Uint8Array) {
      content = await toArrayBuffer(body);
    } else if (typeof body === "string") {
      content = textToArrayBuffer(body);
    } else {
      content = body;
    }
    return content;
  }

  private getKey(fullPath: string) {
    const path = normalizePath(this.rootDir + DIR_SEPARATOR + fullPath);
    const key = getKey(path);
    return key;
  }

  private async toBody(content: Blob | ArrayBuffer) {
    if (typeof content === "string") {
      return content;
    }
    if (typeof process === "object") {
      // Node
      const buffer = await toArrayBuffer(content);
      return Buffer.from(buffer);
    } else {
      // Web
      const blob = toBlob(content);
      return blob;
    }
  }

  // #endregion Private Methods (11)
}
