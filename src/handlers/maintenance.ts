import Context from "../context";
import {
  AlarmRequest,
  AlarmResponse,
  StatusRequest,
  StatusResponse,
} from "@setcd-io/connectrpc-etcd";
import { BaseHandler } from "./base";

export class MaintenanceHandler extends BaseHandler {
  constructor(ctx: Context) {
    super(ctx);
  }

  async alarm(tenant: string, req: AlarmRequest): Promise<AlarmResponse> {
    return {
      $typeName: "etcdserverpb.AlarmResponse",
      header: await this.header(tenant),
      alarms: [],
    };
  }

  async status(tenant: string, req: StatusRequest): Promise<StatusResponse> {
    const size = Number.MAX_SAFE_INTEGER;

    const resp: StatusResponse = {
      $typeName: "etcdserverpb.StatusResponse",
      header: await this.header(tenant),
      dbSize: BigInt(1),
      dbSizeInUse: 0n,
      errors: [],
      version: "3.6.0",
      isLearner: false,
      leader: 0n,
      raftAppliedIndex: 0n,
      raftIndex: 0n,
      raftTerm: 0n,
      dbSizeQuota: BigInt(size),
      storageVersion: "3.6.0",
      downgradeInfo: undefined,
    };

    return resp;
  }
}
