"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ErrGRPCWatchCanceled = exports.ErrGRPCNotImplemented = exports.ErrGRPCCompacted = exports.ErrGRPCKeyNotFound = exports.ErrGRPCEmptyKey = void 0;
const connect_1 = require("@connectrpc/connect");
class ErrGRPCEmptyKey extends connect_1.ConnectError {
    constructor() {
        super("etcdserver: key is not provided", connect_1.Code.InvalidArgument);
    }
}
exports.ErrGRPCEmptyKey = ErrGRPCEmptyKey;
class ErrGRPCKeyNotFound extends connect_1.ConnectError {
    constructor() {
        super("etcdserver: key not found", connect_1.Code.InvalidArgument);
    }
}
exports.ErrGRPCKeyNotFound = ErrGRPCKeyNotFound;
class ErrGRPCCompacted extends connect_1.ConnectError {
    constructor() {
        super("etcdserver: mvcc: required revision has been compacted", connect_1.Code.OutOfRange);
    }
}
exports.ErrGRPCCompacted = ErrGRPCCompacted;
class ErrGRPCNotImplemented extends connect_1.ConnectError {
    constructor(message) {
        super(`Not implemented: ${message}`, connect_1.Code.Unimplemented);
    }
}
exports.ErrGRPCNotImplemented = ErrGRPCNotImplemented;
class ErrGRPCWatchCanceled extends connect_1.ConnectError {
    constructor() {
        super("etcdserver: watch canceled", connect_1.Code.Canceled);
    }
}
exports.ErrGRPCWatchCanceled = ErrGRPCWatchCanceled;
/*
    ErrGRPCEmptyKey                = status.Error(codes.InvalidArgument, "etcdserver: key is not provided")
    ErrGRPCKeyNotFound             = status.Error(codes.InvalidArgument, "etcdserver: key not found")
    ErrGRPCValueProvided           = status.Error(codes.InvalidArgument, "etcdserver: value is provided")
    ErrGRPCLeaseProvided           = status.Error(codes.InvalidArgument, "etcdserver: lease is provided")
    ErrGRPCTooManyOps              = status.Error(codes.InvalidArgument, "etcdserver: too many operations in txn request")
    ErrGRPCDuplicateKey            = status.Error(codes.InvalidArgument, "etcdserver: duplicate key given in txn request")
    ErrGRPCInvalidClientAPIVersion = status.Error(codes.InvalidArgument, "etcdserver: invalid client api version")
    ErrGRPCInvalidSortOption       = status.Error(codes.InvalidArgument, "etcdserver: invalid sort option")
    ErrGRPCCompacted               = status.Error(codes.OutOfRange, "etcdserver: mvcc: required revision has been compacted")
    ErrGRPCFutureRev               = status.Error(codes.OutOfRange, "etcdserver: mvcc: required revision is a future revision")
    ErrGRPCNoSpace                 = status.Error(codes.ResourceExhausted, "etcdserver: mvcc: database space exceeded")

    ErrGRPCLeaseNotFound    = status.Error(codes.NotFound, "etcdserver: requested lease not found")
    ErrGRPCLeaseExist       = status.Error(codes.FailedPrecondition, "etcdserver: lease already exists")
    ErrGRPCLeaseTTLTooLarge = status.Error(codes.OutOfRange, "etcdserver: too large lease TTL")

    ErrGRPCWatchCanceled = status.Error(codes.Canceled, "etcdserver: watch canceled")

    ErrGRPCMemberExist            = status.Error(codes.FailedPrecondition, "etcdserver: member ID already exist")
    ErrGRPCPeerURLExist           = status.Error(codes.FailedPrecondition, "etcdserver: Peer URLs already exists")
    ErrGRPCMemberNotEnoughStarted = status.Error(codes.FailedPrecondition, "etcdserver: re-configuration failed due to not enough started members")
    ErrGRPCMemberBadURLs          = status.Error(codes.InvalidArgument, "etcdserver: given member URLs are invalid")
    ErrGRPCMemberNotFound         = status.Error(codes.NotFound, "etcdserver: member not found")
    ErrGRPCMemberNotLearner       = status.Error(codes.FailedPrecondition, "etcdserver: can only promote a learner member")
    ErrGRPCLearnerNotReady        = status.Error(codes.FailedPrecondition, "etcdserver: can only promote a learner member which is in sync with leader")
    ErrGRPCTooManyLearners        = status.Error(codes.FailedPrecondition, "etcdserver: too many learner members in cluster")
    ErrGRPCClusterIDMismatch      = status.Error(codes.FailedPrecondition, "etcdserver: cluster ID mismatch")
    //revive:disable:var-naming
    // Deprecated: Please use ErrGRPCClusterIDMismatch.
    ErrGRPCClusterIdMismatch = ErrGRPCClusterIDMismatch
    //revive:enable:var-naming

    ErrGRPCRequestTooLarge        = status.Error(codes.InvalidArgument, "etcdserver: request is too large")
    ErrGRPCRequestTooManyRequests = status.Error(codes.ResourceExhausted, "etcdserver: too many requests")

    ErrGRPCRootUserNotExist     = status.Error(codes.FailedPrecondition, "etcdserver: root user does not exist")
    ErrGRPCRootRoleNotExist     = status.Error(codes.FailedPrecondition, "etcdserver: root user does not have root role")
    ErrGRPCUserAlreadyExist     = status.Error(codes.FailedPrecondition, "etcdserver: user name already exists")
    ErrGRPCUserEmpty            = status.Error(codes.InvalidArgument, "etcdserver: user name is empty")
    ErrGRPCUserNotFound         = status.Error(codes.FailedPrecondition, "etcdserver: user name not found")
    ErrGRPCRoleAlreadyExist     = status.Error(codes.FailedPrecondition, "etcdserver: role name already exists")
    ErrGRPCRoleNotFound         = status.Error(codes.FailedPrecondition, "etcdserver: role name not found")
    ErrGRPCRoleEmpty            = status.Error(codes.InvalidArgument, "etcdserver: role name is empty")
    ErrGRPCAuthFailed           = status.Error(codes.InvalidArgument, "etcdserver: authentication failed, invalid user ID or password")
    ErrGRPCPermissionNotGiven   = status.Error(codes.InvalidArgument, "etcdserver: permission not given")
    ErrGRPCPermissionDenied     = status.Error(codes.PermissionDenied, "etcdserver: permission denied")
    ErrGRPCRoleNotGranted       = status.Error(codes.FailedPrecondition, "etcdserver: role is not granted to the user")
    ErrGRPCPermissionNotGranted = status.Error(codes.FailedPrecondition, "etcdserver: permission is not granted to the role")
    ErrGRPCAuthNotEnabled       = status.Error(codes.FailedPrecondition, "etcdserver: authentication is not enabled")
    ErrGRPCInvalidAuthToken     = status.Error(codes.Unauthenticated, "etcdserver: invalid auth token")
    ErrGRPCInvalidAuthMgmt      = status.Error(codes.InvalidArgument, "etcdserver: invalid auth management")
    ErrGRPCAuthOldRevision      = status.Error(codes.InvalidArgument, "etcdserver: revision of auth store is old")

    ErrGRPCNoLeader                   = status.Error(codes.Unavailable, "etcdserver: no leader")
    ErrGRPCNotLeader                  = status.Error(codes.FailedPrecondition, "etcdserver: not leader")
    ErrGRPCLeaderChanged              = status.Error(codes.Unavailable, "etcdserver: leader changed")
    ErrGRPCNotCapable                 = status.Error(codes.FailedPrecondition, "etcdserver: not capable")
    ErrGRPCStopped                    = status.Error(codes.Unavailable, "etcdserver: server stopped")
    ErrGRPCTimeout                    = status.Error(codes.Unavailable, "etcdserver: request timed out")
    ErrGRPCTimeoutDueToLeaderFail     = status.Error(codes.Unavailable, "etcdserver: request timed out, possibly due to previous leader failure")
    ErrGRPCTimeoutDueToConnectionLost = status.Error(codes.Unavailable, "etcdserver: request timed out, possibly due to connection lost")
    ErrGRPCTimeoutWaitAppliedIndex    = status.Error(codes.Unavailable, "etcdserver: request timed out, waiting for the applied index took too long")
    ErrGRPCUnhealthy                  = status.Error(codes.Unavailable, "etcdserver: unhealthy cluster")
    ErrGRPCCorrupt                    = status.Error(codes.DataLoss, "etcdserver: corrupt cluster")
    ErrGRPCNotSupportedForLearner     = status.Error(codes.FailedPrecondition, "etcdserver: rpc not supported for learner")
    ErrGRPCBadLeaderTransferee        = status.Error(codes.FailedPrecondition, "etcdserver: bad leader transferee")

    ErrGRPCWrongDowngradeVersionFormat   = status.Error(codes.InvalidArgument, "etcdserver: wrong downgrade target version format")
    ErrGRPCInvalidDowngradeTargetVersion = status.Error(codes.InvalidArgument, "etcdserver: invalid downgrade target version")
    ErrGRPCClusterVersionUnavailable     = status.Error(codes.FailedPrecondition, "etcdserver: cluster version not found during downgrade")
    ErrGRPCDowngradeInProcess            = status.Error(codes.FailedPrecondition, "etcdserver: cluster has a downgrade job in progress")
    ErrGRPCNoInflightDowngrade           = status.Error(codes.FailedPrecondition, "etcdserver: no inflight downgrade job")

    ErrGRPCCanceled         = status.Error(codes.Canceled, "etcdserver: request canceled")
    ErrGRPCDeadlineExceeded = status.Error(codes.DeadlineExceeded, "etcdserver: context deadline exceeded")
*/
