import { S3 } from "aws-sdk";
import { AbstractAccessor, testAll } from "kura";
import { S3LocalFileSystemAsync } from "../s3/S3LocalFileSystemAsync";

AbstractAccessor.PUT_INDEX_THROTTLE = 0;

const config: S3.ClientConfiguration = {
  accessKeyId: "KFS0LZVKZ8G456A502L3",
  secretAccessKey: "uVwBONMdTwJI1+C8jUhrypvshHz3OY8Ooar3amdC",
  endpoint: "http://127.0.0.1:9000",
  s3ForcePathStyle: true, // needed with minio?
  signatureVersion: "v4"
};
const factory = new S3LocalFileSystemAsync(
  config,
  "web-file-system-test",
  "example",
  { useIndex: true, verbose: true }
);
testAll(factory, async () => {
  const s3 = new S3(config);
  const bucket = "web-file-system-test";
  try {
    await s3.createBucket({ Bucket: bucket }).promise();
  } catch (e) {}
  const list = await s3.listObjectsV2({ Bucket: bucket }).promise();
  for (const content of list.Contents) {
    await s3.deleteObject({ Bucket: bucket, Key: content.Key }).promise();
  }
});
