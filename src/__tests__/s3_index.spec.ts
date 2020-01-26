import {
  blobToString,
  DirectoryEntryAsync,
  FileSystemAsync,
  InvalidModificationError,
  NotFoundError
} from "kura";
import { S3 } from "aws-sdk";
import { S3LocalFileSystemAsync } from "../s3/S3LocalFileSystemAsync";

let fs: FileSystemAsync;
beforeAll(async () => {
  const config: S3.ClientConfiguration = {
    accessKeyId: "KFS0LZVKZ8G456A502L3",
    secretAccessKey: "uVwBONMdTwJI1+C8jUhrypvshHz3OY8Ooar3amdC",
    endpoint: "http://127.0.0.1:9000",
    s3ForcePathStyle: true, // needed with minio?
    signatureVersion: "v4"
  };

  const s3 = new S3(config);
  const bucket = "web-file-system-test";
  try {
    await s3.createBucket({ Bucket: bucket }).promise();
  } catch (e) {}
  const list = await s3.listObjectsV2({ Bucket: bucket }).promise();
  for (const content of list.Contents) {
    await s3.deleteObject({ Bucket: bucket, Key: content.Key }).promise();
  }

  const factory = new S3LocalFileSystemAsync(config, bucket, true);
  fs = await factory.requestFileSystemAsync(
    window.PERSISTENT,
    Number.MAX_VALUE
  );
});

test("add empty file", async done => {
  const fileEntry = await fs.root.getFile("empty.txt", {
    create: true,
    exclusive: true
  });
  expect(fileEntry.name).toBe("empty.txt");
  expect(fileEntry.fullPath).toBe("/empty.txt");
  expect(fileEntry.isDirectory).toBe(false);
  expect(fileEntry.isFile).toBe(true);
  done();
});

test("add text file", async done => {
  let fileEntry = await fs.root.getFile("test.txt", {
    create: true,
    exclusive: true
  });
  expect(fileEntry.name).toBe("test.txt");
  expect(fileEntry.fullPath).toBe("/test.txt");
  expect(fileEntry.isDirectory).toBe(false);
  expect(fileEntry.isFile).toBe(true);

  let writer = await fileEntry.createWriter();
  await writer.write(new Blob(["hoge"], { type: "text/plain" }));
  expect(writer.position).toBe(4);
  let file = await fileEntry.file();
  expect(file.size).toBe(4);

  await writer.write(new Blob(["fuga"], { type: "text/plain" }));
  expect(writer.position).toBe(8);
  file = await fileEntry.file();
  expect(file.size).toBe(8);

  try {
    fileEntry = await fs.root.getFile("test.txt", {
      create: true,
      exclusive: true
    });
    fail();
  } catch (e) {}
  fileEntry = await fs.root.getFile("test.txt");
  file = await fileEntry.file();
  expect(file.size).toBe(8);

  const str = await blobToString(file);
  expect(str).toBe("hogefuga");

  done();
});

test("create dir", async done => {
  const dirEntry = await fs.root.getDirectory("folder", {
    create: true,
    exclusive: true
  });
  expect(dirEntry.isFile).toBe(false);
  expect(dirEntry.isDirectory).toBe(true);
  expect(dirEntry.name).toBe("folder");

  done();
});

test("create file in the dir", async done => {
  const dirEntry = await fs.root.getDirectory("folder");
  const fileEntry = await dirEntry.getFile("in.txt", {
    create: true,
    exclusive: true
  });
  expect(fileEntry.fullPath).toBe("/folder/in.txt");
  try {
    await dirEntry.getFile("out.txt");
    fail();
  } catch (e) {
    expect(e).toBeInstanceOf(NotFoundError);
  }

  const parent = await fileEntry.getParent();
  expect(parent.fullPath).toBe(dirEntry.fullPath);
  expect(parent.name).toBe(dirEntry.name);

  done();
});

test("readdir", async done => {
  const reader = fs.root.createReader();
  const entries = await reader.readEntries();
  let names = ["empty.txt", "test.txt", "folder"];
  for (const entry of entries) {
    names = names.filter(name => name !== entry.name);
  }
  expect(names.length).toBe(0);

  done();
});

test("copy file", async done => {
  const test = await fs.root.getFile("test.txt");
  const testMeta = await test.getMetadata();
  const parent = await test.getParent();
  await test.copyTo(parent, "test2.txt");
  const test2 = await fs.root.getFile("test2.txt");
  const test2Meta = await test2.getMetadata();
  expect(test2Meta.size).toBe(testMeta.size);

  const reader = fs.root.createReader();
  const entries = await reader.readEntries();
  let names = ["empty.txt", "test.txt", "test2.txt", "folder"];
  for (const entry of entries) {
    names = names.filter(name => name !== entry.name);
  }
  expect(names.length).toBe(0);
  done();
});

test("copy folder", async done => {
  const folder = await fs.root.getDirectory("folder");
  const parent = await folder.getParent();
  const folder1 = (await folder.copyTo(
    parent,
    "folder1"
  )) as DirectoryEntryAsync;

  let reader = fs.root.createReader();
  let entries = await reader.readEntries();
  let names = ["empty.txt", "test.txt", "test2.txt", "folder", "folder1"];
  for (const entry of entries) {
    names = names.filter(name => name !== entry.name);
  }
  expect(names.length).toBe(0);

  reader = folder1.createReader();
  entries = await reader.readEntries();
  names = ["in.txt"];
  for (const entry of entries) {
    names = names.filter(name => name !== entry.name);
  }
  expect(names.length).toBe(0);

  done();
});

test("move folder", async done => {
  const folder = await fs.root.getDirectory("folder");
  const parent = await folder.getParent();
  const folder2 = (await folder.moveTo(
    parent,
    "folder2"
  )) as DirectoryEntryAsync;

  let reader = fs.root.createReader();
  let entries = await reader.readEntries();
  let names = ["empty.txt", "test.txt", "test2.txt", "folder1", "folder2"];
  for (const entry of entries) {
    names = names.filter(name => name !== entry.name);
  }
  expect(names.length).toBe(0);

  reader = folder2.createReader();
  entries = await reader.readEntries();
  names = ["in.txt"];
  for (const entry of entries) {
    names = names.filter(name => name !== entry.name);
  }
  expect(names.length).toBe(0);

  done();
});

test("remove a file", async done => {
  const entry = await fs.root.getFile("empty.txt");
  await entry.remove();
  try {
    await fs.root.getFile("empty.txt");
    fail();
  } catch (e) {
    expect(e).toBeInstanceOf(NotFoundError);
  }

  const reader = fs.root.createReader();
  const entries = await reader.readEntries();
  let names = ["test.txt", "test2.txt", "folder1", "folder2"];
  for (const entry of entries) {
    names = names.filter(name => name !== entry.name);
  }
  expect(names.length).toBe(0);

  done();
});

test("remove a not empty folder", async done => {
  const entry = await fs.root.getDirectory("folder2");
  try {
    await entry.remove();
    fail();
  } catch (e) {
    expect(e).toBeInstanceOf(InvalidModificationError);
  }

  done();
});

test("remove recursively", async done => {
  await fs.root.removeRecursively();

  const reader = fs.root.createReader();
  const entries = await reader.readEntries();
  expect(entries.length).toBe(0);

  done();
});