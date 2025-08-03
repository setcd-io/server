"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.MaintenanceHandler = void 0;
const base_1 = require("./base");
class MaintenanceHandler extends base_1.BaseHandler {
    constructor(ctx) {
        super(ctx);
    }
    async alarm(tenant, req) {
        return {
            $typeName: "etcdserverpb.AlarmResponse",
            header: await this.header(tenant),
            alarms: [],
        };
    }
    async status(tenant, req) {
        const size = 0;
        const resp = {
            $typeName: "etcdserverpb.StatusResponse",
            header: await this.header(tenant),
            dbSize: BigInt(size),
            dbSizeInUse: 0n,
            errors: [],
            version: "3.5.0",
            isLearner: false,
            leader: 0n,
            raftAppliedIndex: 0n,
            raftIndex: 0n,
            raftTerm: 0n,
        };
        return resp;
    }
}
exports.MaintenanceHandler = MaintenanceHandler;
