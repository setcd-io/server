"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const context_1 = require("./context");
const fastify_1 = require("fastify");
const connect_fastify_1 = require("@connectrpc/connect-fastify");
const routes_1 = require("./routes");
const connect_1 = require("@connectrpc/connect");
// import { TableMonitor } from "./storage/monitor";
const rxjs_1 = require("rxjs");
const kv_1 = require("./handlers/kv");
const watch_1 = require("./handlers/watch");
const lease_1 = require("./handlers/lease");
const maintenance_1 = require("./handlers/maintenance");
const cluster_1 = require("./handlers/cluster");
const auth_1 = require("./handlers/auth");
const nanoid_1 = require("nanoid");
const log_1 = require("./util/log");
const const_1 = require("./util/const");
const isStatus = process.argv.slice(-1)[0] === "status";
const isVersion = process.argv.includes("--version");
const isHttp2 = !process.argv.includes("--no-http2");
const isLocal = !process.env.AWS_LAMBDA_RUNTIME_API;
const intercept = (next) => async (req) => {
    req.contextValues?.set(const_1.CONNECTION_ID, (0, nanoid_1.nanoid)(8));
    const token = req.header.get("token");
    if (!token) {
        req.contextValues?.set(const_1.TENANT, const_1.DEFAULT_TENANT); // TODO: Throw error
    }
    else {
        const [name] = Buffer.from(token, "base64").toString("utf8").split(":");
        req.contextValues?.set(const_1.TENANT, name);
    }
    return (0, log_1.logRequest)(req, next).catch((err) => {
        if (err instanceof connect_1.ConnectError) {
            throw err;
        }
        throw new connect_1.ConnectError(err.message, connect_1.Code.Unknown);
    });
};
async function main(ctx) {
    if (isVersion) {
        await ctx.version();
        return;
    }
    // if (isStatus) {
    //   await ctx.status();
    //   return;
    // }
    console.log("\nStarting Server...");
    let https = {
        key: await ctx.keyfile(),
        cert: await ctx.certfile(),
        ca: await ctx.certfile(),
        allowHTTP1: true,
    };
    const server = (isHttp2
        ? (0, fastify_1.fastify)({
            http2: true,
            https,
            keepAliveTimeout: const_1.KEEP_ALIVE_TIMEOUT,
            // forceCloseConnections: "idle",
            connectionTimeout: const_1.CONNECTION_TIMEOUT,
            requestTimeout: const_1.REQUEST_TIMEOUT,
        })
        : (0, fastify_1.fastify)({
            https,
            keepAliveTimeout: const_1.KEEP_ALIVE_TIMEOUT,
            forceCloseConnections: "idle",
            connectionTimeout: const_1.CONNECTION_TIMEOUT,
            requestTimeout: const_1.REQUEST_TIMEOUT,
        }));
    // const tableMonitor = new TableMonitor(ctx);
    const auth = new auth_1.AuthHandler(ctx);
    const lease = new lease_1.LeaseHandler(ctx);
    const kv = new kv_1.KVHandler(ctx, lease);
    const watch = new watch_1.WatchHandler(ctx, kv);
    const maintenance = new maintenance_1.MaintenanceHandler(ctx);
    const cluster = new cluster_1.ClusterHandler(ctx);
    const kvStorage = await ctx.kvStorage;
    const revisionStorage = await ctx.revisionStorage;
    const historyStorage = await (0, rxjs_1.firstValueFrom)(ctx.historyStorage);
    console.table({
        "KV Table": kvStorage.tableArn,
        "Revision Table": revisionStorage.tableArn,
        "History Table": historyStorage.tableArn,
    });
    const routes = (0, routes_1.createRouter)({ auth, kv, lease, watch, maintenance, cluster });
    await server.register(connect_fastify_1.fastifyConnectPlugin, {
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
    console.info("Server Listening:", `${server
        .addresses()
        .map((addr) => `${addr.address}:${addr.port}`)
        .join(" ")}\n`);
    const stats = (0, rxjs_1.interval)(1000, rxjs_1.asyncScheduler).subscribe(() => {
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
                }
                else {
                    console.log("Server Exited");
                    process.exit();
                }
            });
        });
    }
}
void main(context_1.context)
    .then(() => { })
    .catch(console.error);
