import Context, { context } from "./context";
import { fastify, FastifyInstance } from "fastify";
import { fastifyConnectPlugin } from "@connectrpc/connect-fastify";
import { createRouter } from "./routes";
import { Code, ConnectError, Interceptor } from "@connectrpc/connect";
import http2 from "http2-wrapper";
// import { TableMonitor } from "./storage/monitor";
import { asyncScheduler, interval } from "rxjs";
import { KVHandler } from "./handlers/kv";
import { WatchHandler } from "./handlers/watch";
import { LeaseHandler } from "./handlers/lease";
import { MaintenanceHandler } from "./handlers/maintenance";
import { ClusterHandler } from "./handlers/cluster";
import { AuthHandler } from "./handlers/auth";
import { nanoid } from "nanoid";
import _ from "lodash";
import { log } from "./util/log";
import {
  CONNECTION_ID,
  CONNECTION_TIMEOUT,
  DEFAULT_TENANT,
  KEEP_ALIVE_TIMEOUT,
  REQUEST_TIMEOUT,
  TENANT,
} from "./util/const";
import { Shards } from "./cloud-rx/dynamodb/shards";
import { TenantHistory } from "./storage/kv";

const isStatus = process.argv.slice(-1)[0] === "status";
const isVersion = process.argv.includes("--version");
const isHttp2 = !process.argv.includes("--no-http2");
const isLocal = !process.env.AWS_LAMBDA_RUNTIME_API;

const intercept: Interceptor = (next) => async (req) => {
  req.contextValues?.set(CONNECTION_ID, nanoid(8));

  const token = req.header.get("token");
  if (!token) {
    req.contextValues?.set(TENANT, DEFAULT_TENANT); // TODO: Throw error
  } else {
    const [name] = Buffer.from(token, "base64").toString("utf8").split(":");
    req.contextValues?.set(TENANT, name);
  }

  return log(req, next).catch((err) => {
    if (err instanceof ConnectError) {
      throw err;
    }
    throw new ConnectError(err.message, Code.Unknown);
  });
};

async function main(ctx: Context) {
  if (isVersion) {
    await ctx.version();
    return;
  }

  // if (isStatus) {
  //   await ctx.status();
  //   return;
  // }

  console.log("\nStarting Server...");

  let https: http2.SecureServerOptions | boolean = {
    key: await ctx.keyfile(),
    cert: await ctx.certfile(),
    ca: await ctx.certfile(),
    allowHTTP1: true,
  };

  const server = (
    isHttp2
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
  const kv = new KVHandler(ctx);
  const lease = new LeaseHandler(ctx, kv);
  const watch = new WatchHandler(ctx, kv);
  const maintenance = new MaintenanceHandler(ctx);
  const cluster = new ClusterHandler(ctx);

  const kvStorage = await ctx.kvStorage.init("kv");
  const revisionStorage = await ctx.revisionStorage.init("revision");
  const historyStorage = await ctx.historyStorage.init("history");

  console.table({
    "KV Table": kvStorage.repr(),
    "Revision Table": revisionStorage.repr(),
    "History Table": historyStorage.repr(),
  });

  const routes = createRouter({ auth, kv, lease, watch, maintenance, cluster });

  await server.register(fastifyConnectPlugin, {
    routes,
    interceptors: [intercept],
  });

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

  const stats = interval(1000, asyncScheduler).subscribe(() => {
    watch.stats();
  });

  if (isLocal) {
    // const sub = tableMonitor.records$
    //   .pipe(
    //     switchMap((event) =>
    //       from(
    //         axios.post("https://localhost:2379/events/dynamodb", event, {
    //           validateStatus: () => true,
    //           httpsAgent: new Agent({
    //             checkServerIdentity: () => undefined,
    //             rejectUnauthorized: false,
    //             cert: undefined,
    //             key: undefined,
    //           }),
    //           headers: {
    //             Host: "dynamodb.amazonaws.com",
    //             "Content-Type": "application/json",
    //           },
    //         })
    //       )
    //     )
    //   )
    //   .subscribe(() => {});

    ctx.on("abort", ({ code }) => {
      console.log("Server Stopping...");
      // sub.unsubscribe();
      stats.unsubscribe();
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
}

void main(context)
  .then(() => {})
  .catch(console.error);
