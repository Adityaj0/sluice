use std::future::Future;
use std::pin::Pin;

use sluice_core::{Event, now_millis};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_stream::Stream;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

use crate::proto::ingest_server::{Ingest, IngestServer};
use crate::proto::{BatchAck, EventBatch, EventRequest, IngestResponse};
use crate::router::EventRouter;

/// The gRPC `Ingest` service.
pub struct IngestService {
    router: EventRouter,
}

impl IngestService {
    pub fn new(router: EventRouter) -> Self {
        Self { router }
    }

    pub fn into_server(self) -> IngestServer<Self> {
        IngestServer::new(self)
    }
}

fn envelope(key: Vec<u8>, value: Vec<u8>, event_time: u64, now: u64) -> Event<Vec<u8>> {
    Event {
        key: (!key.is_empty()).then_some(key),
        value,
        event_time: if event_time == 0 { now } else { event_time },
        ingestion_time: now,
    }
}

#[tonic::async_trait]
impl Ingest for IngestService {
    type IngestStreamStream = Pin<Box<dyn Stream<Item = Result<BatchAck, Status>> + Send + 'static>>;

    async fn ingest_stream(
        &self,
        request: Request<Streaming<EventBatch>>,
    ) -> Result<Response<Self::IngestStreamStream>, Status> {
        let mut inbound = request.into_inner();
        let router = self.router.clone();
        let (acks, rx) = mpsc::channel(16);
        tokio::spawn(async move {
            loop {
                let batch = match inbound.message().await {
                    Ok(Some(batch)) => batch,
                    Ok(None) => break,
                    Err(status) => {
                        let _ = acks.send(Err(status)).await;
                        break;
                    }
                };
                if batch.stream.is_empty() {
                    let _ = acks.send(Err(Status::invalid_argument("`stream` is required"))).await;
                    break;
                }
                let now = now_millis();
                let count = batch.events.len() as u32;
                let events = batch
                    .events
                    .into_iter()
                    .map(|e| envelope(e.key, e.value, e.event_time, now))
                    .collect();
                // Not reading the next batch until this one is stored is
                // what makes backpressure reach the producer.
                let ack = match router.route_batch(&batch.stream, events).await {
                    Ok(first) => Ok(BatchAck {
                        first_offset: first,
                        last_offset: first + u64::from(count.saturating_sub(1)),
                        count,
                    }),
                    Err(e) => Err(Status::failed_precondition(e.to_string())),
                };
                let failed = ack.is_err();
                if acks.send(ack).await.is_err() || failed {
                    break;
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn ingest_event(&self, request: Request<EventRequest>) -> Result<Response<IngestResponse>, Status> {
        let request = request.into_inner();
        if request.stream.is_empty() {
            return Err(Status::invalid_argument("`stream` is required"));
        }

        let event = envelope(request.key, request.value, request.event_time, now_millis());

        let offset = self
            .router
            .route(&request.stream, event)
            .await
            .map_err(|e| Status::failed_precondition(e.to_string()))?;
        Ok(Response::new(IngestResponse { offset }))
    }
}

/// Serves the ingest service on `listener` until `shutdown` completes.
pub async fn serve<F>(listener: TcpListener, router: EventRouter, shutdown: F) -> Result<(), tonic::transport::Error>
where
    F: Future<Output = ()>,
{
    Server::builder()
        .add_service(IngestService::new(router).into_server())
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown)
        .await
}
