use sluice_ingest::proto::{Event, EventBatch, EventRequest};
use sluice_ingest::proto::ingest_client::IngestClient;
use sluice_ingest::{EventRouter, RawEvent, ReadResult, Subscription, serve};
use tokio::net::TcpListener;
use tonic::Code;

async fn read(subscription: &Subscription, offset: u64) -> RawEvent {
    match subscription.log().read_wait(offset).await {
        ReadResult::Event(event) => event,
        other => panic!("expected an event at {offset}, got {other:?}"),
    }
}

async fn start() -> (EventRouter, IngestClient<tonic::transport::Channel>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let router = EventRouter::new();
    tokio::spawn(serve(listener, router.clone(), std::future::pending()));
    let client = IngestClient::connect(format!("http://{addr}")).await.unwrap();
    (router, client)
}

#[tokio::test]
async fn ingested_events_reach_subscribers() {
    let (router, mut client) = start().await;
    let sub = router.subscribe("purchases");

    for i in 0..3u64 {
        let response = client
            .ingest_event(EventRequest {
                stream: "purchases".into(),
                key: format!("user-{i}").into_bytes(),
                value: format!(r#"{{"amount": {i}}}"#).into_bytes(),
                event_time: 1_000 + i,
            })
            .await
            .unwrap();
        assert_eq!(response.into_inner().offset, i);
    }

    for i in 0..3u64 {
        let event = read(&sub, i).await;
        assert_eq!(event.key_bytes(), format!("user-{i}").as_bytes());
        assert_eq!(event.value, format!(r#"{{"amount": {i}}}"#).into_bytes());
        assert_eq!(event.event_time, 1_000 + i);
        assert!(event.ingestion_time > 0);
    }
}

#[tokio::test]
async fn missing_event_time_defaults_to_ingestion_time() {
    let (router, mut client) = start().await;
    let sub = router.subscribe("s");
    client
        .ingest_event(EventRequest {
            stream: "s".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    let event = read(&sub, 0).await;
    assert_eq!(event.event_time, event.ingestion_time);
    assert_eq!(event.key, None);
}

#[tokio::test]
async fn rejects_bad_requests() {
    let (_router, mut client) = start().await;

    let status = client.ingest_event(EventRequest::default()).await.unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);

    let status = client
        .ingest_event(EventRequest {
            stream: "unknown".into(),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::FailedPrecondition);
    assert!(status.message().contains("unknown"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handles_10k_events() {
    let (router, mut client) = start().await;
    let sub = std::sync::Arc::new(router.subscribe("load"));
    let consumer = tokio::spawn({
        let sub = std::sync::Arc::clone(&sub);
        async move {
            let mut n = 0u64;
            while n < 10_000 {
                let event = read(&sub, n).await;
                assert_eq!(event.value, n.to_le_bytes());
                n += 1;
                sub.commit(n);
            }
            n
        }
    });
    for i in 0..10_000u64 {
        client
            .ingest_event(EventRequest {
                stream: "load".into(),
                value: i.to_le_bytes().to_vec(),
                event_time: i + 1,
                ..Default::default()
            })
            .await
            .unwrap();
    }
    assert_eq!(consumer.await.unwrap(), 10_000);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn streams_batches_with_acks() {
    let (router, mut client) = start().await;
    let sub = router.subscribe("load");
    let batches: Vec<EventBatch> = (0..20u64)
        .map(|b| EventBatch {
            stream: "load".into(),
            events: (0..500u64)
                .map(|i| Event {
                    key: vec![],
                    value: (b * 500 + i).to_le_bytes().to_vec(),
                    event_time: b * 500 + i + 1,
                })
                .collect(),
        })
        .collect();
    let mut acks = client
        .ingest_stream(tokio_stream::iter(batches))
        .await
        .unwrap()
        .into_inner();
    let mut expected_first = 0;
    while let Some(ack) = acks.message().await.unwrap() {
        assert_eq!((ack.first_offset, ack.last_offset, ack.count), (expected_first, expected_first + 499, 500));
        expected_first += 500;
    }
    assert_eq!(expected_first, 10_000);
    for offset in [0u64, 4_999, 9_999] {
        assert_eq!(read(&sub, offset).await.value, offset.to_le_bytes());
    }
}

#[tokio::test]
async fn stream_errors_for_unknown_streams() {
    let (_router, mut client) = start().await;
    let batch = EventBatch { stream: "nobody".into(), events: vec![Event::default()] };
    let mut acks = client.ingest_stream(tokio_stream::iter(vec![batch])).await.unwrap().into_inner();
    let status = acks.message().await.unwrap_err();
    assert_eq!(status.code(), Code::FailedPrecondition);
}
