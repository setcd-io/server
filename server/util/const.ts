import { createContextKey } from "@connectrpc/connect";

export const CONNECTION_ID = createContextKey("connectionId");
export const TENANT = createContextKey("connectionId");

export const KEEP_ALIVE_TIMEOUT = 20000;
export const CONNECTION_TIMEOUT = 5000;
export const REQUEST_TIMEOUT = 5000;

export const DEFAULT_TENANT = "default";
