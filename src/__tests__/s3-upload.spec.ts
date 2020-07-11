import "kura";
import { S3 } from "aws-sdk";
import { testAll } from "kura/lib/__tests__/filesystem";
import { S3LocalFileSystemAsync } from "../s3/S3LocalFileSystemAsync";

const config: S3.ClientConfiguration = {
  accessKeyId: "minioadmin",
  secretAccessKey: "minioadmin",
  endpoint: "http://127.0.0.1:9000",
  s3ForcePathStyle: true, // needed with minio?
  signatureVersion: "v4",
};
const factory = new S3LocalFileSystemAsync(
  config,
  "web-file-system-test",
  "/example/",
  {
    verbose: true,
    methodOfDoPutContent: "upload",
  }
);
testAll(
  factory,
  async () => {
    const s3 = new S3(config);
    const bucket = "web-file-system-test";
    try {
      await s3.createBucket({ Bucket: bucket }).promise();
    } catch (e) {}
    const list = await s3.listObjectsV2({ Bucket: bucket }).promise();
    for (const content of list.Contents) {
      await s3.deleteObject({ Bucket: bucket, Key: content.Key }).promise();
    }
  },
  true
);
