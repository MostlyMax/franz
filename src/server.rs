use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Duration};

use axum::{
    body::Body,
    extract::{
        ws::{close_code, CloseFrame, Message, Utf8Bytes, WebSocket},
        Path, State, WebSocketUpgrade,
    },
    http::StatusCode,
    response::Response,
};
use disk_chan::{Consumer, Producer};
use tokio::{
    net::{TcpListener, TcpStream},
    select,
};
use tokio_stream::StreamExt;
use tokio_util::{
    codec::{FramedRead, LinesCodec},
    sync::CancellationToken,
    task::TaskTracker,
};
use tracing::{debug, error, info};

use crate::{try_handshake, Config};

pub struct AppState {
    _data_dir: PathBuf,
    ws_tracker: TaskTracker,
    shutdown: CancellationToken,
    topics: HashMap<String, disk_chan::Producer>,
}

impl AppState {
    pub async fn cleanup_with_timeout(&self, timeout: Duration) {
        self.shutdown.cancel();
        self.ws_tracker.close();
        select! {
            _ = self.ws_tracker.wait() => {},
            _ = tokio::time::sleep(timeout) => {
                error!("failed to shutdown gracefully, some tasks are still running");
                error!("forcefully removing lock on disk-chan - this can potentially cause data corruption");
                let _ = std::fs::remove_file(self._data_dir.join(".pid.lock"));
            },
        };
    }

    pub async fn new(shutdown: CancellationToken, config: Config) -> Self {
        let mut topics = HashMap::new();

        for t in config.topic {
            let producer = disk_chan::new(config.data_dir.join(&t.name), t.page_size, t.max_pages)
                .await
                .unwrap();

            topics.insert(t.name, producer);
        }

        AppState {
            _data_dir: config.data_dir,
            ws_tracker: TaskTracker::new(),
            shutdown,
            topics,
        }
    }
}

async fn handle_produce_attempt_2(
    mut sock: TcpStream,
    mut consumer: Consumer,
    shutdown: CancellationToken,
) {
    let codec = LinesCodec::new();
    let mut framed = FramedRead::new(sock, codec);

    loop {
        let msg = select! {
            biased;

            // important: shutdown token must be first due to biased select
            _ = shutdown.cancelled() => break,
            msg = framed.next() =>  {
                match msg {
                    Some(Ok(_)) => continue,
                    Some(Err(_)) => break,
                    None => break,
                }
            },
            msg = consumer.recv() => msg,
        };
    }
}

async fn handle_consume(mut ws: WebSocket, mut consumer: Consumer, shutdown: CancellationToken) {
    loop {
        let msg = select! {
            biased;

            // important: shutdown token must be first due to biased select
            _ = shutdown.cancelled() => break,
            msg = ws.recv() =>  {
                match msg {
                    Some(Ok(_)) => continue,
                    Some(Err(_)) => break,
                    None => break,
                }
            },
            msg = consumer.recv() => msg,
        };

        let data = match msg {
            Some(data) => data,
            None => {
                if let Err(e) = consumer.next_page().await {
                    let _ = ws
                        .send(Message::Close(Some(CloseFrame {
                            code: close_code::ERROR,
                            reason: Utf8Bytes::from("failed to get next page"),
                        })))
                        .await;

                    error!(%e);

                    return;
                };
                continue;
            }
        };

        if ws.send(data.into()).await.is_err() {
            debug!("client closed connection");
            return;
        }
    }

    let _ = ws
        .send(Message::Close(Some(CloseFrame {
            code: close_code::AWAY,
            reason: Utf8Bytes::from("server shutdown"),
        })))
        .await;
}

pub async fn consume(
    ws: WebSocketUpgrade,
    Path((topic, group)): Path<(String, usize)>,
    State(state): State<Arc<AppState>>,
) -> Response {
    let Some(producer) = state.topics.get(&topic) else {
        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from(format!(
                "topic with name {topic} does not exist"
            )))
            .expect("to never fail");
    };

    let Ok(consumer) = producer.subscribe(group).await else {
        return Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Body::from(format!(
                "unable to subscribe to topic with group {group}"
            )))
            .expect("to never fail");
    };

    ws.on_upgrade(move |sock| {
        let shutdown = state.shutdown.clone();

        state
            .ws_tracker
            .track_future(handle_consume(sock, consumer, shutdown))
    })
}

async fn handle_produce(mut ws: WebSocket, mut producer: Producer, shutdown: CancellationToken) {
    const REASONABLE_CLEANUP_MESSAGE_LIMIT: usize = 10_000;

    loop {
        let msg = select! {
            biased;

            // important: shutdown token must be first due to biased select
            _ = shutdown.cancelled() => break,
            msg = ws.recv() => msg,
        };

        let data = match msg {
            Some(Ok(data)) => data,
            Some(Err(_)) => break,
            None => break,
        };

        if let Err(e) = producer.send(data.into_data()).await {
            let _ = ws
                .send(Message::Close(Some(CloseFrame {
                    code: close_code::ERROR,
                    reason: Utf8Bytes::from_static("failed to produce message"),
                })))
                .await;

            error!(%e);

            return;
        }
    }

    let _ = ws
        .send(Message::Close(Some(CloseFrame {
            code: close_code::AWAY,
            reason: Utf8Bytes::from("server shutdown"),
        })))
        .await;

    //for _ in 0..REASONABLE_CLEANUP_MESSAGE_LIMIT {
    //    let data = match ws.recv().await {
    //        Some(Ok(data)) => data,
    //        Some(Err(_)) => break,
    //        None => break,
    //    };
    //
    //    if let Err(e) = producer.send(data.into_data()).await {
    //        error!(%e, "failed to produce message on cleanup");
    //
    //        return;
    //    }
    //}
}

pub async fn produce(
    ws: WebSocketUpgrade,
    Path(topic): Path<String>,
    State(state): State<Arc<AppState>>,
) -> Response {
    let Some(producer) = state.topics.get(&topic) else {
        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from(format!(
                "topic with name {topic} does not exist"
            )))
            .expect("to never fail");
    };

    let producer = producer.clone();

    ws.on_upgrade(move |sock| {
        let shutdown = state.shutdown.clone();

        state
            .ws_tracker
            .track_future(handle_produce(sock, producer, shutdown))
    })
}

pub async fn serve(listener: TcpListener, state: Arc<AppState>) -> Result<(), std::io::Error> {
    while let Ok((sock, _)) = listener.accept().await {
        let hs = try_handshake(sock).await.unwrap();

        eprintln!("{:#?}", hs);
    }
    Ok(())
}
