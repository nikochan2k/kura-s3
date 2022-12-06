import { AWSError } from "aws-sdk";
import S3, {
  ClientConfiguration,
  CompletedMultipartUpload,
  CompleteMultipartUploadRequest,
  CreateMultipartUploadRequest,
  DeleteObjectRequest,
  GetObjectRequest,
  ListObjectsV2Request,
  UploadPartRequest,
} from "aws-sdk/clients/s3";
import {
  AbstractAccessor,
  DIR_SEPARATOR,
  FileSystem,
  FileSystemObject,
  getName,
  INDEX_DIR_PATH,
  InvalidModificationError,
  isBlob,
  normalizePath,
  NotFoundError,
  NotReadableError,
  toArrayBuffer,
  toBlob,
  toBuffer,
  XHR,
  XHROptions,
} from "kura";
import { FileSystemOptions } from "kura/lib/FileSystemOptions";
import { S3FileSystem } from "./S3FileSystem";
import { S3FileSystemOptions } from "./S3FileSystemOption";
import { getKey, getPrefix } from "./S3Util";

interface UrlCache {
  expirationTime: number;
  url: string;
}

const EXPIRES = 60 * 60 * 24 * 7;

const isBrowser =
  typeof window !== "undefined" && typeof window.document !== "undefined";

const isNode =
  typeof process !== "undefined" &&
  process.versions != null &&
  process.versions.node != null;

const isReactNative =
  typeof navigator !== "undefined" && navigator.product === "ReactNative";

const hasBuffer = typeof Buffer === "function";

export class S3Accessor extends AbstractAccessor {
  private urlCache: { [key: string]: UrlCache } = {};

  public filesystem: FileSystem;
  public name: string;
  public s3: S3;

  constructor(
    private config: ClientConfiguration,
    private bucket: string,
    private rootDir: string,
    private s3Options?: S3FileSystemOptions
  ) {
    super(s3Options);
    if (!s3Options.expires) {
      s3Options.expires = EXPIRES;
    }
    if (!config.httpOptions) {
      config.httpOptions = {};
    }
    config.maxRetries = 0;
    if (config.httpOptions.timeout == null) {
      config.httpOptions.timeout = 1000;
      config.httpOptions.connectTimeout = 1000;
    }
    config.s3ForcePathStyle = true;
    config.signatureVersion = "v4";
    this.s3 = new S3(config);
    this.filesystem = new S3FileSystem(this);
    if (!this.rootDir.startsWith(DIR_SEPARATOR)) {
      this.rootDir = DIR_SEPARATOR + this.rootDir;
    }
    this.name = this.bucket + this.rootDir;
  }

  public async createIndexDir(dirPath: string) {
    let indexDir = INDEX_DIR_PATH + dirPath;
    if (!indexDir.endsWith(DIR_SEPARATOR)) {
      indexDir += DIR_SEPARATOR;
    }

    return indexDir;
  }

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
      if (this.isNotFoundError(err)) {
        return;
      }
      throw new InvalidModificationError(this.name, fullPath, err);
    }
  }

  public async doGetObject(fullPath: string): Promise<FileSystemObject> {
    const key = this.getKey(fullPath);
    if (this.s3Options.getObjectUsingListObject) {
      const params: ListObjectsV2Request = {
        Bucket: this.bucket,
        Prefix: key,
      };
      try {
        var data = await this.s3.listObjectsV2(params).promise();
      } catch (err) {
        this.handleNotFoundErrorS3(fullPath, err);
        throw new NotReadableError(this.name, fullPath, err);
      }
      if (data.KeyCount === 0) {
        throw new NotFoundError(this.name, fullPath);
      }
      for (const content of data.Contents) {
        if (content.Key === key) {
          return {
            name: getName(fullPath),
            fullPath: fullPath,
            lastModified: content.LastModified.getTime(),
            size: content.Size,
          };
        }
      }
      throw new NotFoundError(this.name, fullPath);
    } else {
      try {
        const data = await this.s3
          .headObject({
            Bucket: this.bucket,
            Key: key,
          })
          .promise();
        const name = key.split(DIR_SEPARATOR).pop();
        return {
          name,
          fullPath: fullPath,
          lastModified: data.LastModified.getTime(),
          size: data.ContentLength,
        };
      } catch (err) {
        this.handleNotFoundErrorS3(fullPath, err);
        throw new NotReadableError(this.name, fullPath, err);
      }
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

  public async doMakeDirectory(_fullPath: string) {
    // NOOP
  }

  public async doReadContent(
    fullPath: string
  ): Promise<Blob | BufferSource | string> {
    if (this.s3Options.methodOfDoGetContent === "xhr") {
      return await this.doReadContentUsingXHR(
        fullPath,
        isBrowser ? "blob" : "arraybuffer"
      );
    } else {
      return await this.doReadContentUsingGetObject(fullPath);
    }
  }

  public async getURL(
    fullPath: string,
    method?: "GET" | "POST" | "PUT" | "DELETE"
  ): Promise<string> {
    const keysToRemove: string[] = [];
    const now = Math.trunc(Date.now() / 1000);
    for (const [key, cache] of Object.entries(this.urlCache)) {
      if (cache.expirationTime <= now) {
        keysToRemove.push(key);
      }
    }
    for (const keyToRemove of keysToRemove) {
      delete this.urlCache[keyToRemove];
    }

    if (!method || method === "GET") {
      const key = fullPath + "|get";
      const cache = this.urlCache[key];
      if (cache) {
        return cache.url;
      }
      const url = await this.getSignedUrl(fullPath, "getObject");
      this.urlCache[key] = {
        expirationTime: now + this.s3Options.expires,
        url,
      };
      return url;
    } else if (method === "PUT") {
      const key = fullPath + "|put";
      const cache = this.urlCache[key];
      if (cache) {
        return cache.url;
      }
      const url = await this.getSignedUrl(fullPath, "putObject");
      this.urlCache[key] = {
        expirationTime: now + this.s3Options.expires,
        url,
      };
      return url;
    } else {
      return null;
    }
  }

  protected doWriteArrayBuffer(
    fullPath: string,
    buffer: ArrayBuffer
  ): Promise<void> {
    return this.doWriteContentToS3(fullPath, buffer);
  }

  protected async doWriteBase64(
    fullPath: string,
    base64: string
  ): Promise<void> {
    return this.doWriteContentToS3(fullPath, base64);
  }

  protected doWriteBlob(fullPath: string, blob: Blob): Promise<void> {
    return this.doWriteContentToS3(fullPath, blob);
  }

  protected async doWriteBuffer(
    fullPath: string,
    buffer: Buffer
  ): Promise<void> {
    return this.doWriteContentToS3(fullPath, buffer);
  }

  protected initialize(options: FileSystemOptions) {
    this.initializeIndexOptions(options);

    if (options.contentsCache == null) {
      options.contentsCache = false;
    }
    this.initializeContentsCacheOptions(options);

    this.debug("S3Accessor#initialize", JSON.stringify(options));
  }

  protected initializeIndexOptions(options: FileSystemOptions) {
    if (!options.index) {
      return;
    }

    if (options.indexOptions == null) {
      options.indexOptions = {};
    }

    const indexOptions = options.indexOptions;
    if (indexOptions.noCache == null) {
      indexOptions.noCache = true;
    }
    if (indexOptions.logicalDelete == null) {
      indexOptions.logicalDelete = false;
    }
  }

  private async doReadContentUsingGetObject(fullPath: string) {
    try {
      const key = this.getKey(fullPath);
      const req: GetObjectRequest = {
        Bucket: this.bucket,
        Key: key,
      };
      if (this.s3Options.noCache) {
        req.ResponseCacheControl = "no-cache";
        req.ResponseExpires = new Date(0);
      }
      const data = await this.s3.getObject(req).promise();
      return this.fromBody(data.Body);
    } catch (err) {
      this.handleNotFoundErrorS3(fullPath, err);
      throw new NotReadableError(this.name, fullPath, err);
    }
  }

  private async doReadContentUsingXHR(
    fullPath: string,
    responseType: XMLHttpRequestResponseType
  ) {
    const xhrOptions: XHROptions = {
      timeout: this.config.httpOptions.timeout,
    };
    if (this.s3Options.noCache) {
      xhrOptions.requestHeaders["Cache-Control"] = "no-cache";
    }
    const xhr = new XHR(this.name, fullPath, xhrOptions);
    const url = await this.getSignedUrl(fullPath, "getObject");
    return xhr.get(url, responseType);
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
      this.handleNotFoundErrorS3(dirPath, err);
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

  private async doWriteContentToS3(
    fullPath: string,
    content: Blob | BufferSource | string
  ) {
    const method = this.s3Options.methodOfDoPutContent;

    if (typeof content === "string") {
      if (hasBuffer) {
        content = await toBuffer(content);
      } else {
        content = await toBlob(content);
      }
    }

    if (method === "uploadPart") {
      content = await toArrayBuffer(content);
      await this.doWriteContentUsingUploadPart(fullPath, content);
    } else if (method === "xhr") {
      await this.doWriteContentUsingXHR(fullPath, content);
    } else if (method === "upload") {
      await this.doWriteContentUsingUpload(fullPath, content);
    } else {
      await this.doWriteContentUsingPutObject(fullPath, content);
    }
  }

  private async doWriteContentUsingPutObject(
    fullPath: string,
    content: Blob | BufferSource
  ) {
    const body = await this.toBody(content);
    const key = this.getKey(fullPath);
    const contentLength = isBlob(content) ? content.size : content.byteLength;
    try {
      const data = await this.s3
        .putObject({
          Bucket: this.bucket,
          Key: key,
          Body: body,
          ContentLength: contentLength,
        })
        .promise();
    } catch (err) {
      throw new InvalidModificationError(this.name, fullPath, err);
    }
  }

  private async doWriteContentUsingUpload(
    fullPath: string,
    content: Blob | BufferSource
  ) {
    const body = await this.toBody(content);
    const key = this.getKey(fullPath);
    const contentLength = isBlob(content) ? content.size : content.byteLength;
    await this.s3
      .upload({
        Bucket: this.bucket,
        Key: key,
        Body: body,
        ContentLength: contentLength,
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
    content: Blob | BufferSource
  ) {
    try {
      const url = await this.getSignedUrl(fullPath, "putObject");
      const xhr = new XHR(this.name, fullPath, {
        timeout: this.config.httpOptions.timeout,
      });
      if (isBlob(content) || ArrayBuffer.isView(content)) {
        await xhr.put(url, content);
      } else {
        const view = new Uint8Array(content);
        await xhr.put(url, view);
      }
    } catch (err) {
      throw new InvalidModificationError(this.name, fullPath, err);
    }
  }

  private async fromBody(body: any): Promise<BufferSource | Blob | string> {
    if (isReactNative) {
      return toArrayBuffer(body);
    } else {
      return body;
    }
  }

  private getKey(fullPath: string) {
    const path = normalizePath(this.rootDir + DIR_SEPARATOR + fullPath);
    const key = getKey(path);
    return key;
  }

  private async getSignedUrl(
    fullPath: string,
    operation: "getObject" | "putObject"
  ) {
    const key = this.getKey(fullPath);
    const url = await this.s3.getSignedUrlPromise(operation, {
      Bucket: this.bucket,
      Key: key,
      Expires: this.s3Options.expires,
    });
    return url;
  }

  private handleNotFoundErrorS3(fullPath: string, err: any) {
    if (this.isNotFoundError(err)) {
      throw new NotFoundError(this.name, fullPath, err);
    }
  }

  private isNotFoundError(err: any) {
    if (!err) {
      return false;
    }

    const awsError: AWSError = err;
    if (awsError.statusCode === 404) {
      return true;
    }
    return false;
  }

  private async toBody(content: Blob | BufferSource) {
    if (isNode) {
      return toBuffer(content);
    }

    if (isReactNative) {
      if (hasBuffer) {
        return toBuffer(content);
      } else {
        return toArrayBuffer(content);
      }
    }

    return toBlob(content);
  }
}
