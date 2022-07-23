import { FileSystemOptions } from "kura/lib/FileSystemOptions";
import { XHROptions } from "kura";

export interface S3FileSystemOptions extends FileSystemOptions {
  expires?: number;
  getObjectUsingListObject?: boolean;
  methodOfDoGetContent?: "xhr" | "getObject";
  methodOfDoPutContent?: "xhr" | "upload" | "uploadPart" | "putObject";
  noCache?: boolean;
  xhrOptions?: XHROptions;
}
