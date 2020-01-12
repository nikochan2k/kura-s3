import { AbstractDirectoryReader, FileSystemObject } from "kura";
import { S3Accessor } from "./S3Accessor";
import { S3DirectoryEntry } from "./S3DirectoryEntry";
import { S3FileEntry } from "./S3FileEntry";

export class S3DirectoryReader extends AbstractDirectoryReader<S3Accessor> {
  constructor(public dirEntry: S3DirectoryEntry) {
    super(dirEntry);
  }

  protected createEntry(obj: FileSystemObject) {
    return obj.size != null
      ? new S3FileEntry({
          accessor: this.dirEntry.params.accessor,
          ...obj
        })
      : new S3DirectoryEntry({
          accessor: this.dirEntry.params.accessor,
          ...obj
        });
  }
}
