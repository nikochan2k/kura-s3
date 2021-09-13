import { FileSystemOptions } from "kura/lib/FileSystemOptions";
import { XHROptions } from "kura";

export interface S3FileSystemOptions extends FileSystemOptions {
  // #region Properties (3)

  noCache?: boolean;
  methodOfDoGetContent?: "xhr" | "getObject";
  methodOfDoPutContent?: "xhr" | "upload" | "uploadPart" | "putObject";
  getObjectUsingListObject?: boolean;
  xhrOptions?: XHROptions;

  // #endregion Properties (3)
}
