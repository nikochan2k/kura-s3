import { DIR_SEPARATOR } from "kura";

export function getKey(fullPath: string) {
  let key = "";
  if (0 < fullPath.length) {
    key = fullPath.substr(1);
  }
  return key;
}

export function getPrefix(fullPath: string) {
  let key = getKey(fullPath);
  if (key) {
    key += DIR_SEPARATOR;
  }
  return key;
}
