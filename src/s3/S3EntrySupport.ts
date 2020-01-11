import {
  AbstractEntrySupport,
  DirectoryEntry,
  FileEntry,
  FileSystemObject,
  FileSystemParams
} from "kura";
import { S3DirectoryEntry } from "./S3DirectoryEntry";
import { S3FileEntry } from "./S3FileEntry";
import { S3FileSystem } from "./S3FileSystem";

export class S3EntrySupport extends AbstractEntrySupport {
  constructor(private params: FileSystemParams<S3FileSystem>) {
    super();
  }

  getDirectoryObject(path: string): Promise<FileSystemObject> {
    throw new Error("Method not implemented.");
  }

  getFileObject(path: string): Promise<FileSystemObject> {
    throw new Error("Method not implemented.");
  }

  toDirectoryEntry(obj: FileSystemObject): DirectoryEntry {
    return new S3DirectoryEntry({
      filesystem: this.params.filesystem,
      ...obj
    });
  }

  toFileEntry(obj: FileSystemObject): FileEntry {
    return new S3FileEntry({
      filesystem: this.params.filesystem,
      ...obj
    });
  }

  toURL(): string {
    throw new Error("Method not implemented.");
  }
}
