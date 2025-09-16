import Context, { context } from "./context";
import {
  fastify,
  FastifyInstance,
  FastifyReply,
  FastifyRequest,
} from "fastify";
import { fastifyConnectPlugin } from "@connectrpc/connect-fastify";
import { createRouter } from "./routes";
import { Code, ConnectError, Interceptor } from "@connectrpc/connect";
import http2 from "http2-wrapper";
import { PeerCertificate, TLSSocket } from "tls";
// import { TableMonitor } from "./storage/monitor";
import { asyncScheduler, firstValueFrom, interval } from "rxjs";
import { KVHandler } from "./handlers/kv";
import { WatchHandler } from "./handlers/watch";
import { LeaseHandler } from "./handlers/lease";
import { MaintenanceHandler } from "./handlers/maintenance";
import { ClusterHandler } from "./handlers/cluster";
import { AuthHandler } from "./handlers/auth";
import { nanoid } from "nanoid";
import _ from "lodash";
import { logRequest } from "./util/log";
import {
  CONNECTION_ID,
  CONNECTION_TIMEOUT,
  KEEP_ALIVE_TIMEOUT,
  NAMESPACE,
  REQUEST_TIMEOUT,
  TENANT,
} from "./util/const";
import { authHook, X_NAMESPACE, X_TENANT } from "./auth";
import { ErrGRPCAuthFailed } from "./util/error";

const intercept: Interceptor = (next) => async (req) => {
  req.contextValues?.set(CONNECTION_ID, nanoid(8));
  const namespace = req.header.get(X_NAMESPACE);
  const tenant = req.header.get(X_TENANT);

  if (!namespace && !tenant) {
    throw new ErrGRPCAuthFailed();
  }

  req.contextValues?.set(NAMESPACE, namespace);
  req.contextValues?.set(TENANT, tenant);

  return logRequest(req, next).catch((err) => {
    if (err instanceof ConnectError) {
      throw err;
    }
    throw new ConnectError(err.message, Code.Unknown);
  });
};

async function main(ctx: Context) {
  console.log(await ctx.repr());
  console.log("\nStarting Server...");

  let https: http2.SecureServerOptions | boolean = {
    key: ctx.keyfile(),
    cert: ctx.certfile(),
    ca: ctx.certfile(),
    requestCert: true,
    rejectUnauthorized: false,
    allowHTTP1: true,
  };

  const server = (
    ctx.env.isHttp2
      ? fastify({
          http2: true,
          https,
          keepAliveTimeout: KEEP_ALIVE_TIMEOUT,
          // forceCloseConnections: "idle",
          connectionTimeout: CONNECTION_TIMEOUT,
          requestTimeout: REQUEST_TIMEOUT,
        })
      : fastify({
          https,
          keepAliveTimeout: KEEP_ALIVE_TIMEOUT,
          forceCloseConnections: "idle",
          connectionTimeout: CONNECTION_TIMEOUT,
          requestTimeout: REQUEST_TIMEOUT,
        })
  ) as FastifyInstance;

  // const tableMonitor = new TableMonitor(ctx);
  const auth = new AuthHandler(ctx);
  const lease = new LeaseHandler(ctx);
  const kv = new KVHandler(ctx, lease);
  const watch = new WatchHandler(ctx, kv);
  const maintenance = new MaintenanceHandler(ctx);
  const cluster = new ClusterHandler(ctx);

  const routes = createRouter({ auth, kv, lease, watch, maintenance, cluster });

  await server.register(fastifyConnectPlugin, {
    routes,
    interceptors: [intercept],
  });

  server.addHook("onRequest", authHook);
  server.post("/events/dynamodb", kv.dynamodbHandler());

  server.get("/health", (_, reply) => {
    reply.code(200).send({ health: "true", reason: "" });
  });

  await server.listen({
    host: "0.0.0.0",
    port: 2379,
    signal: ctx.signal,
  });

  console.info(
    "Server Listening:",
    `${server
      .addresses()
      .map((addr) => `${addr.address}:${addr.port}`)
      .join(" ")}\n`
  );

  ctx.on("abort", ({ code }) => {
    console.log("Server Stopping...");
    server.close();
    process.nextTick(() => {
      if (code) {
        console.log("Server Exited with code", code);
        process.exit(code);
      } else {
        console.log("Server Exited");
        process.exit();
      }
    });
  });
}

void main(context)
  .then(() => {})
  .catch(console.error);
