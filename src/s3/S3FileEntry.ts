import {
  AbstractFileEntry,
  DirectoryEntry,
  FileSystemObject,
  FileSystemParams,
} from "kura";
import { S3Accessor } from "./S3Accessor";
import { S3DirectoryEntry } from "./S3DirectoryEntry";
import { S3FileWriter } from "./S3FileWriter";

export interface S3FileParams extends FileSystemParams<S3Accessor> {
  size: number;
}

export class S3FileEntry extends AbstractFileEntry<S3Accessor> {
  constructor(params: FileSystemParams<S3Accessor>) {
    super(params);
  }

  protected createFileWriter(file: File): S3FileWriter {
    return new S3FileWriter(this, file);
  }

  protected toDirectoryEntry(obj: FileSystemObject): DirectoryEntry {
    return new S3DirectoryEntry({
      accessor: this.params.accessor,
      ...obj,
    });
  }
}
