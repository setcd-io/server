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
    const size = 0;

    const resp: StatusResponse = {
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
