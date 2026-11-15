use std::sync::Arc;

use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use std::path::PathBuf;

use axum::extract::ws::{Message, WebSocketUpgrade};
use sluice_runtime::OperatorMetricsSnapshot;
use tower_http::services::{ServeDir, ServeFile};
use serde::Deserialize;
use serde_json::json;
use tower_http::cors::CorsLayer;

use crate::job_def::JobDefinition;
use crate::job_manager::{CheckpointsView, JobDetails, JobError, JobId, JobManager, JobSummary, OutputRecord};

type AppState = State<Arc<JobManager>>;

/// The REST API.
///
/// | Method | Path                  | Purpose                                 |
/// |--------|-----------------------|-----------------------------------------|
/// | POST   | `/jobs`               | Submit a JSON job definition            |
/// | GET    | `/jobs`               | List jobs                               |
/// | GET    | `/jobs/:id`           | Status, definition and operator metrics |
/// | DELETE | `/jobs/:id`           | Cancel a job                            |
/// | GET    | `/jobs/:id/metrics`   | Per-operator metrics                    |
/// | GET    | `/jobs/:id/output`    | Most recent sink output (`?limit=N`)    |
/// | GET    | `/jobs/:id/checkpoints` | Checkpoint history and statistics     |
/// | GET    | `/jobs/:id/checkpoints/:n/state` | State as of checkpoint n     |
/// | GET    | `/jobs/:id/live`      | WebSocket: live metrics every 500 ms    |
/// | POST   | `/jobs/:id/stages/:stage/kill` | Kill a stage to test recovery  |
/// | GET    | `/streams`            | Ingest streams and their subscribers    |
/// | POST   | `/sql/explain`        | Compile a SQL query and show its plans  |
/// | POST   | `/streams/:name/events` | Send JSON records into a stream       |
/// | GET/POST | `/generators`       | List / start synthetic data generators  |
/// | DELETE | `/generators/:id`     | Stop a generator                        |
/// | GET    | `/metrics`            | Prometheus exposition                   |
/// | GET    | `/health`             | Liveness                                |
pub fn app(manager: Arc<JobManager>) -> Router {
    app_with_ui(manager, None)
}

/// The API, plus the built frontend (`frontend/dist`) served for every
/// other path.
pub fn app_with_ui(manager: Arc<JobManager>, ui_dir: Option<PathBuf>) -> Router {
    let router = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/metrics", get(prometheus))
        .route("/streams", get(streams))
        .route("/streams/:stream/events", post(send_events))
        .route("/generators", get(list_generators).post(start_generator))
        .route("/generators/:id", axum::routing::delete(stop_generator))
        .route("/sql/explain", post(explain_sql))
        .route("/jobs", get(list_jobs).post(submit_job))
        .route("/jobs/:id", get(get_job).delete(cancel_job))
        .route("/jobs/:id/metrics", get(job_metrics))
        .route("/jobs/:id/output", get(job_output))
        .route("/jobs/:id/checkpoints", get(job_checkpoints))
        .route("/jobs/:id/checkpoints/:checkpoint/state", get(checkpoint_state))
        .route("/jobs/:id/live", get(live))
        .route("/jobs/:id/stages/:stage/kill", post(kill_stage))
        .layer(CorsLayer::permissive())
        .with_state(manager);
    match ui_dir {
        Some(dir) => {
            let index = ServeFile::new(dir.join("index.html"));
            router.fallback_service(ServeDir::new(dir).not_found_service(index))
        }
        None => router,
    }
}

/// Serves the REST API on `listener` until `shutdown` completes.
pub async fn serve<F>(
    listener: tokio::net::TcpListener,
    manager: Arc<JobManager>,
    ui_dir: Option<PathBuf>,
    shutdown: F,
) -> std::io::Result<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    axum::serve(listener, app_with_ui(manager, ui_dir)).with_graceful_shutdown(shutdown).await
}

struct ApiError {
    status: StatusCode,
    message: String,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(json!({ "error": self.message }))).into_response()
    }
}

impl From<JobError> for ApiError {
    fn from(error: JobError) -> Self {
        let status = match error {
            JobError::Invalid(_) => StatusCode::BAD_REQUEST,
            JobError::NotFound(_) | JobError::StageNotFound { .. } | JobError::CheckpointUnavailable(_) => {
                StatusCode::NOT_FOUND
            }
            JobError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        Self {
            status,
            message: error.to_string(),
        }
    }
}

impl From<JsonRejection> for ApiError {
    fn from(rejection: JsonRejection) -> Self {
        Self {
            status: rejection.status(),
            message: rejection.body_text(),
        }
    }
}

async fn submit_job(
    State(manager): AppState,
    body: Result<Json<JobDefinition>, JsonRejection>,
) -> Result<(StatusCode, Json<JobSummary>), ApiError> {
    let Json(definition) = body?;
    let summary = manager.submit(definition)?;
    Ok((StatusCode::CREATED, Json(summary)))
}

async fn list_jobs(State(manager): AppState) -> Json<Vec<JobSummary>> {
    Json(manager.list())
}

async fn get_job(State(manager): AppState, Path(id): Path<JobId>) -> Result<Json<JobDetails>, ApiError> {
    Ok(Json(manager.details(id)?))
}

async fn cancel_job(State(manager): AppState, Path(id): Path<JobId>) -> Result<(StatusCode, Json<JobSummary>), ApiError> {
    Ok((StatusCode::ACCEPTED, Json(manager.cancel(id)?)))
}

async fn job_metrics(
    State(manager): AppState,
    Path(id): Path<JobId>,
) -> Result<Json<Vec<OperatorMetricsSnapshot>>, ApiError> {
    Ok(Json(manager.metrics(id)?))
}

async fn job_checkpoints(State(manager): AppState, Path(id): Path<JobId>) -> Result<Json<CheckpointsView>, ApiError> {
    Ok(Json(manager.checkpoints(id)?))
}

#[derive(Deserialize)]
struct StateQuery {
    limit: Option<usize>,
}

async fn checkpoint_state(
    State(manager): AppState,
    Path((id, checkpoint)): Path<(JobId, u64)>,
    Query(query): Query<StateQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    Ok(Json(manager.inspect(id, checkpoint, query.limit.unwrap_or(500)).await?))
}

/// Streams [`JobManager::live`] snapshots until the client goes away.
async fn live(State(manager): AppState, Path(id): Path<JobId>, ws: WebSocketUpgrade) -> Result<Response, ApiError> {
    manager.live(id)?; // 404 before upgrading
    Ok(ws.on_upgrade(move |mut socket| async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_millis(500));
        loop {
            // Also listen, so a client's close frame is answered and the
            // stream stops instead of pushing into a half-closed socket.
            tokio::select! {
                _ = ticker.tick() => {}
                message = socket.recv() => match message {
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                    Some(Ok(_)) => continue,
                },
            }
            let Ok(snapshot) = manager.live(id) else { break };
            if socket.send(Message::Text(snapshot.to_string())).await.is_err() {
                return;
            }
        }
        let _ = socket.close().await;
    }))
}

async fn kill_stage(
    State(manager): AppState,
    Path((id, stage)): Path<(JobId, usize)>,
) -> Result<(StatusCode, Json<JobSummary>), ApiError> {
    Ok((StatusCode::ACCEPTED, Json(manager.kill_stage(id, stage)?)))
}

#[derive(Deserialize)]
struct OutputQuery {
    limit: Option<usize>,
}

async fn job_output(
    State(manager): AppState,
    Path(id): Path<JobId>,
    Query(query): Query<OutputQuery>,
) -> Result<Json<Vec<OutputRecord>>, ApiError> {
    Ok(Json(manager.output(id, query.limit.unwrap_or(100))?))
}

#[derive(Deserialize)]
struct ExplainRequest {
    query: String,
}

/// Compiles a query without running it: logical plan, optimizations,
/// optimized plan and physical stages, or the error with its position.
async fn explain_sql(body: Result<Json<ExplainRequest>, JsonRejection>) -> Response {
    let request = match body {
        Ok(Json(request)) => request,
        Err(rejection) => return ApiError::from(rejection).into_response(),
    };
    match sluice_sql::compile(&request.query) {
        Ok(compiled) => Json(compiled.to_json()).into_response(),
        Err(error) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": error.to_string(), "details": error })),
        )
            .into_response(),
    }
}

/// Accepts one JSON record or an array of them; each becomes an event
/// timestamped now.
async fn send_events(
    State(manager): AppState,
    Path(stream): Path<String>,
    body: Result<Json<serde_json::Value>, JsonRejection>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let Json(body) = body?;
    let records = match body {
        serde_json::Value::Array(records) => records,
        record => vec![record],
    };
    if records.is_empty() {
        return Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            message: "send at least one record".into(),
        });
    }
    let now = sluice_core::now_millis();
    let count = records.len();
    let events = records
        .into_iter()
        .map(|record| sluice_core::Event::new(record.to_string().into_bytes(), now))
        .collect();
    let first = manager.router().route_batch(&stream, events).await.map_err(|e| ApiError {
        status: StatusCode::CONFLICT,
        message: format!("{e}: deploy a job that reads it first"),
    })?;
    Ok(Json(json!({ "stream": stream, "first_offset": first, "count": count })))
}

async fn list_generators(State(manager): AppState) -> Json<Vec<crate::generators::GeneratorInfo>> {
    Json(manager.generators().list())
}

async fn start_generator(
    State(manager): AppState,
    body: Result<Json<crate::generators::GeneratorSpec>, JsonRejection>,
) -> Result<(StatusCode, Json<crate::generators::GeneratorInfo>), ApiError> {
    let Json(spec) = body?;
    manager
        .generators()
        .start(spec)
        .map(|info| (StatusCode::CREATED, Json(info)))
        .map_err(|message| ApiError {
            status: StatusCode::BAD_REQUEST,
            message,
        })
}

async fn stop_generator(State(manager): AppState, Path(id): Path<u64>) -> Result<Json<crate::generators::GeneratorInfo>, ApiError> {
    manager.generators().stop(id).map(Json).ok_or_else(|| ApiError {
        status: StatusCode::NOT_FOUND,
        message: format!("generator {id} not found"),
    })
}

async fn streams(State(manager): AppState) -> Json<serde_json::Value> {
    let streams: Vec<_> = manager
        .router()
        .streams()
        .into_iter()
        .map(|s| {
            json!({
                "name": s.name,
                "subscribers": s.subscribers,
                "next_offset": s.next_offset,
                "retained": s.retained,
            })
        })
        .collect();
    Json(json!(streams))
}

async fn prometheus(State(manager): AppState) -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        crate::metrics::render(&manager),
    )
}
