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
    const resp: StatusResponse = {
      $typeName: "etcdserverpb.StatusResponse",
      header: await this.header(tenant),
      dbSize: BigInt(1),
      dbSizeInUse: BigInt(1),
      errors: [],
      version: "3.6.0",
      isLearner: false,
      leader: BigInt(1),
      raftAppliedIndex: BigInt(1),
      raftIndex: BigInt(1),
      raftTerm: BigInt(1),
      dbSizeQuota: BigInt(0),
      storageVersion: "3.6.0",
      downgradeInfo: {
        $typeName: "etcdserverpb.DowngradeInfo",
        enabled: false,
        targetVersion: "",
      },
    };

    return resp;
  }
}
