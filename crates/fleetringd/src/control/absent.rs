//! The `Control` service a node that runs no control plane serves, so that a control call sent to
//! a data-only node gets a clear answer instead of a bare `UNIMPLEMENTED`.

use fleetring_proto::v1::control_server::Control;
use fleetring_proto::v1::{
    AcquireLeaseRequest, ChangeDurabilityRequest, ChangeDurabilityResponse,
    CompletePromotionRequest, CompletePromotionResponse, CreateDatabaseRequest,
    CreateDatabaseResponse, DropDatabaseRequest, DropDatabaseResponse, HeartbeatRequest,
    HeartbeatResponse, JoinRequest, JoinResponse, LeaseResponse, LeaveRequest, LeaveResponse,
    ListDatabasesRequest, ListDatabasesResponse, LocateRequest, LocateResponse, PromotionResponse,
    RecordFencedRequest, RecordFencedResponse, RenewLeaseRequest, StartPromotionRequest,
    WatchMembershipRequest,
};
use tonic::{Request, Response, Status};

use crate::control::service::MembershipStream;

/// A `Control` service that answers every call with `FAILED_PRECONDITION`, naming the control
/// node this node is configured to use.
#[derive(Debug, Clone)]
pub struct NoControl {
    control: String,
}

impl NoControl {
    /// A service that points callers at the control node at `control` (`host:port`).
    #[must_use]
    pub fn new(control: impl Into<String>) -> Self {
        Self {
            control: control.into(),
        }
    }

    fn refuse<T>(&self) -> Result<Response<T>, Status> {
        Err(Status::failed_precondition(format!(
            "this node runs no control plane; the control node is {}",
            self.control
        )))
    }
}

#[tonic::async_trait]
impl Control for NoControl {
    type WatchMembershipStream = MembershipStream;

    async fn join(&self, _: Request<JoinRequest>) -> Result<Response<JoinResponse>, Status> {
        self.refuse()
    }

    async fn heartbeat(
        &self,
        _: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        self.refuse()
    }

    async fn watch_membership(
        &self,
        _: Request<WatchMembershipRequest>,
    ) -> Result<Response<Self::WatchMembershipStream>, Status> {
        self.refuse()
    }

    async fn create_database(
        &self,
        _: Request<CreateDatabaseRequest>,
    ) -> Result<Response<CreateDatabaseResponse>, Status> {
        self.refuse()
    }

    async fn drop_database(
        &self,
        _: Request<DropDatabaseRequest>,
    ) -> Result<Response<DropDatabaseResponse>, Status> {
        self.refuse()
    }

    async fn change_durability(
        &self,
        _: Request<ChangeDurabilityRequest>,
    ) -> Result<Response<ChangeDurabilityResponse>, Status> {
        self.refuse()
    }

    async fn locate(&self, _: Request<LocateRequest>) -> Result<Response<LocateResponse>, Status> {
        self.refuse()
    }

    async fn list_databases(
        &self,
        _: Request<ListDatabasesRequest>,
    ) -> Result<Response<ListDatabasesResponse>, Status> {
        self.refuse()
    }

    async fn acquire_lease(
        &self,
        _: Request<AcquireLeaseRequest>,
    ) -> Result<Response<LeaseResponse>, Status> {
        self.refuse()
    }

    async fn renew_lease(
        &self,
        _: Request<RenewLeaseRequest>,
    ) -> Result<Response<LeaseResponse>, Status> {
        self.refuse()
    }

    async fn leave(&self, _: Request<LeaveRequest>) -> Result<Response<LeaveResponse>, Status> {
        self.refuse()
    }

    async fn start_promotion(
        &self,
        _: Request<StartPromotionRequest>,
    ) -> Result<Response<PromotionResponse>, Status> {
        self.refuse()
    }

    async fn record_fenced(
        &self,
        _: Request<RecordFencedRequest>,
    ) -> Result<Response<RecordFencedResponse>, Status> {
        self.refuse()
    }

    async fn complete_promotion(
        &self,
        _: Request<CompletePromotionRequest>,
    ) -> Result<Response<CompletePromotionResponse>, Status> {
        self.refuse()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn every_call_names_the_control_node() {
        let service = NoControl::new("10.0.0.1:7400");
        let status = service
            .locate(Request::new(LocateRequest {
                database: "acme".into(),
            }))
            .await
            .expect_err("refused");
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            status.message(),
            "this node runs no control plane; the control node is 10.0.0.1:7400"
        );
    }
}
