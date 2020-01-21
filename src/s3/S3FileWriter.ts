import { AbstractFileWriter } from "kura";
import { S3Accessor } from "./S3Accessor";
import { S3FileEntry } from "./S3FileEntry";

export class S3FileWriter extends AbstractFileWriter<S3Accessor> {
  constructor(fileEntry: S3FileEntry, file: File) {
    super(fileEntry, file);
  }
}
