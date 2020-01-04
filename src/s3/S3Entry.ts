import {
  DirectoryEntry,
  DirectoryEntryCallback,
  Entry,
  EntryCallback,
  ErrorCallback,
  FileSystemParams,
  Metadata,
  MetadataCallback,
  NotImplementedError,
  VoidCallback
} from "kura";
import { S3FileSystem } from "./S3FileSystem";

export abstract class S3Entry implements Entry {
  abstract isFile: boolean;
  abstract isDirectory: boolean;
  get name() {
    return this.params.name;
  }
  get fullPath() {
    return this.params.fullPath;
  }
  get filesystem() {
    return this.params.filesystem;
  }

  constructor(public params: FileSystemParams<S3FileSystem>) {}

  getMetadata(
    successCallback: MetadataCallback,
    errorCallback?: ErrorCallback
  ): void {
    successCallback({
      modificationTime:
        this.params.lastModified == null
          ? null
          : new Date(this.params.lastModified),
      size: this.params.size
    });
  }

  setMetadata(
    metadata: Metadata,
    successCallback: VoidCallback,
    errorCallback?: ErrorCallback
  ): void {
    throw new NotImplementedError(this.filesystem.name, this.fullPath);
  }

  moveTo(
    parent: DirectoryEntry,
    newName?: string,
    successCallback?: EntryCallback,
    errorCallback?: ErrorCallback
  ): void {
    throw new NotImplementedError(this.filesystem.name, this.fullPath);
  }

  copyTo(
    parent: DirectoryEntry,
    newName?: string,
    successCallback?: EntryCallback,
    errorCallback?: ErrorCallback
  ): void {
    throw new NotImplementedError(this.filesystem.name, this.fullPath);
  }

  toURL(): string {
    throw new Error("Method not implemented.");
  }

  abstract remove(
    successCallback: VoidCallback,
    errorCallback?: ErrorCallback
  ): void;

  getParent(
    successCallback: DirectoryEntryCallback,
    errorCallback?: ErrorCallback
  ): void {
    throw new Error("Method not implemented.");
  }
}
