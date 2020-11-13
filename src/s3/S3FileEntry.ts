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
  // #region Properties (1)

  size: number;

  // #endregion Properties (1)
}

export class S3FileEntry extends AbstractFileEntry<S3Accessor> {
  // #region Constructors (1)

  constructor(params: FileSystemParams<S3Accessor>) {
    super(params);
  }

  // #endregion Constructors (1)

  // #region Protected Methods (2)

  protected createFileWriter(file: File): S3FileWriter {
    return new S3FileWriter(this, file);
  }

  protected toDirectoryEntry(obj: FileSystemObject): DirectoryEntry {
    return new S3DirectoryEntry({
      accessor: this.params.accessor,
      ...obj,
    });
  }

  // #endregion Protected Methods (2)
}
