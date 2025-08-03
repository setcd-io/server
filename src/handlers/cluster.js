"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ClusterHandler = void 0;
const base_1 = require("./base");
class ClusterHandler extends base_1.BaseHandler {
    constructor(ctx) {
        super(ctx);
    }
    async members(tenant, req, ctx) {
        return {
            $typeName: "etcdserverpb.MemberListResponse",
            header: await this.header(tenant),
            members: [
                {
                    $typeName: "etcdserverpb.Member",
                    clientURLs: [ctx.url],
                    ID: 0n,
                    isLearner: false,
                    name: tenant,
                    peerURLs: [ctx.url],
                },
            ],
        };
    }
}
exports.ClusterHandler = ClusterHandler;
