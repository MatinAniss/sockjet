use std::{collections::HashMap, env, sync::Arc};

use axum::{
    extract::{
        ws::{Message, WebSocket},
        Path, Query, WebSocketUpgrade,
    },
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Extension, Json, Router,
};
use futures::{SinkExt, StreamExt};
use serde::{ser::SerializeStruct, Deserialize, Serialize, Serializer};
use tokio::{
    fs,
    net::TcpListener,
    sync::{
        mpsc::{self, UnboundedSender},
        RwLock,
    },
};
use uuid::Uuid;

struct AppStateInner {
    apps: RwLock<HashMap<String, App>>,
}

enum AppStatus {
    Enabled,
    Disabled,
}

struct App {
    status: RwLock<AppStatus>,
    secret: String,
    channels: RwLock<HashMap<String, Channel>>,
}

impl App {
    fn new(secret: String) -> App {
        App {
            status: RwLock::new(AppStatus::Enabled),
            secret,
            channels: RwLock::new(HashMap::new()),
        }
    }
}

struct Channel {
    subscribers: RwLock<HashMap<Uuid, UnboundedSender<Message>>>,
}

impl Channel {
    fn new() -> Channel {
        Channel {
            subscribers: RwLock::new(HashMap::new()),
        }
    }

    async fn add_subscriber(&self, socket_id: Uuid, tx: UnboundedSender<Message>) {
        self.subscribers.write().await.insert(socket_id, tx);
    }

    async fn remove_subscriber(&self, socket_id: &Uuid) {
        self.subscribers.write().await.remove(socket_id);
    }
}

type AppState = Extension<Arc<AppStateInner>>;

#[derive(Deserialize)]
struct Config {
    port: u16,
    apps: Vec<AppConfig>,
}

#[derive(Deserialize)]
struct AppConfig {
    key: String,
    secret: String,
    channels: Vec<String>,
}

#[tokio::main]
async fn main() {
    let app_state = AppStateInner {
        apps: RwLock::new(HashMap::new()),
    };

    // Load config
    let config_string = fs::read_to_string(
        env::current_dir()
            .expect("Failed to get current directory")
            .join("config.json"),
    )
    .await
    .expect("Missing config.json");
    let config = serde_json::from_str::<Config>(&config_string).expect("Invalid config.json");

    for app in config.apps {
        app_state
            .apps
            .write()
            .await
            .insert(app.key.clone(), App::new(app.secret));

        let apps_lock = app_state.apps.read().await;
        let app_in_state = apps_lock.get(&app.key).unwrap();
        for channel in app.channels {
            app_in_state
                .channels
                .write()
                .await
                .insert(channel, Channel::new());
        }
    }

    let app = Router::new()
        .route("/app/:id", get(ws_handler))
        .route("/apps/:id/events", post(post_event))
        .layer(Extension(Arc::new(app_state)));

    let address = format!("127.0.0.1:{}", config.port);
    let listener = TcpListener::bind(address).await.unwrap();

    axum::serve(listener, app.into_make_service())
        .await
        .unwrap();
}

#[derive(Deserialize)]
struct PostEventPayload {
    name: String,
    data: String,
    channel: String,
}

#[derive(Serialize)]
struct PostEventResponse {
    status: u64,
    message: String,
}

#[derive(Serialize)]
struct ServerEvent {
    event: String,
    channel: String,
    data: String,
}

#[derive(Deserialize)]
struct AuthParams {
    auth_key: Option<String>,
    auth_timestamp: Option<u64>,
    auth_version: Option<u64>,
}

async fn post_event(
    Path(path): Path<String>,
    state: AppState,
    auth: Query<AuthParams>,
    Json(payload): Json<PostEventPayload>,
) -> impl IntoResponse {
    let apps_lock = state.apps.read().await;
    let app = apps_lock.get(&path);

    match app {
        Some(app) => match auth.auth_key.as_ref() {
            Some(auth_key) => {
                if &app.secret == auth_key {
                    let channels_lock = app.channels.read().await;
                    let channel = channels_lock.get(&payload.channel);

                    match channel {
                        Some(channel) => {
                            let subscribers = &*channel.subscribers.write().await;

                            let event = ServerEvent {
                                event: payload.name,
                                channel: payload.channel,
                                data: payload.data,
                            };

                            for subscriber in subscribers {
                                let _ = subscriber
                                    .1
                                    .send(Message::Text(serde_json::to_string(&event).unwrap()));
                            }

                            (
                                StatusCode::OK,
                                Json(PostEventResponse {
                                    status: 200,
                                    message: String::from("Success"),
                                }),
                            )
                        }
                        None => (
                            StatusCode::BAD_REQUEST,
                            Json(PostEventResponse {
                                status: 400,
                                message: String::from("Channel does not exist"),
                            }),
                        ),
                    }
                } else {
                    (
                        StatusCode::UNAUTHORIZED,
                        Json(PostEventResponse {
                            status: 401,
                            message: String::from("Unauthorized"),
                        }),
                    )
                }
            }
            None => (
                StatusCode::UNAUTHORIZED,
                Json(PostEventResponse {
                    status: 401,
                    message: String::from("Missing auth_key query paramater"),
                }),
            ),
        },
        None => (
            StatusCode::BAD_REQUEST,
            Json(PostEventResponse {
                status: 400,
                message: String::from("App does not exist"),
            }),
        ),
    }
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    Path(path): Path<String>,
    state: AppState,
) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_socket(socket, path, state))
}

enum ServerMessage {
    ConnectionEstablished(ConnectionEstablishedData),
    SubscriptionSucceeded(String, String),
    SubscriptionError(String, SubscriptionErrorData),
    Pong,
    Error(ErrorData),
}

impl Serialize for ServerMessage {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            ServerMessage::ConnectionEstablished(data) => {
                let mut connection_established =
                    serializer.serialize_struct("ConnectionEstablished", 2)?;
                connection_established.serialize_field("event", "pusher:connection_established")?;
                connection_established.serialize_field("data", &data)?;
                connection_established.end()
            }
            ServerMessage::SubscriptionSucceeded(channel, data) => {
                let mut subscription_succeeded =
                    serializer.serialize_struct("SubscriptionSucceeded", 3)?;
                subscription_succeeded
                    .serialize_field("event", "pusher_internal:subscription_succeeded")?;
                subscription_succeeded.serialize_field("channel", &channel)?;
                subscription_succeeded.serialize_field("data", &data)?;
                subscription_succeeded.end()
            }
            ServerMessage::SubscriptionError(channel, data) => {
                let mut subscription_succeeded =
                    serializer.serialize_struct("SubscriptionError", 3)?;
                subscription_succeeded
                    .serialize_field("event", "pusher_internal:subscription_error")?;
                subscription_succeeded.serialize_field("channel", &channel)?;
                subscription_succeeded.serialize_field("data", &data)?;
                subscription_succeeded.end()
            }
            ServerMessage::Pong => {
                let mut pong = serializer.serialize_struct("Pong", 2)?;
                pong.serialize_field("event", "pusher:pong]")?;
                pong.serialize_field("data", "{}")?;
                pong.end()
            }
            ServerMessage::Error(data) => {
                let mut error = serializer.serialize_struct("Error", 2)?;
                error.serialize_field("event", "pusher:error")?;
                error.serialize_field("data", &data)?;
                error.end()
            }
        }
    }
}

#[derive(Serialize)]
struct ConnectionEstablishedData {
    socket_id: String,
    activity_timeout: u64,
}

#[derive(Serialize)]
struct SubscriptionErrorData {
    #[serde(rename = "type")]
    error_type: String,
    message: String,
    status: u64,
}

#[derive(Serialize)]
struct ErrorData {
    message: String,
    code: u64,
}

async fn handle_socket(mut socket: WebSocket, app_id: String, state: AppState) {
    let app_exists = state.apps.read().await.get(&app_id).is_some();

    if app_exists {
        let socket_id = Uuid::new_v4();
        let connection_established =
            ServerMessage::ConnectionEstablished(ConnectionEstablishedData {
                socket_id: socket_id.to_string(),
                activity_timeout: 120,
            });

        if socket
            .send(Message::Text(
                serde_json::to_string(&connection_established).unwrap(),
            ))
            .await
            .is_err()
        {
            // Disconnected before connection established
            return;
        }

        let (mut tx, mut rx) = socket.split();
        let (send_tx, mut send_rx) = mpsc::unbounded_channel::<Message>();

        let mut send_task = tokio::spawn(async move {
            while let Some(msg) = send_rx.recv().await {
                let _ = tx.send(msg).await;
            }
        });

        let mut recv_task = tokio::spawn(async move {
            while let Some(Ok(msg)) = rx.next().await {
                handle_socket_message(&socket_id, &send_tx, msg, &app_id, &state).await;
            }
        });

        loop {
            tokio::select! {
                _send = (&mut send_task) => {
                    recv_task.abort();
                },
                _recv = (&mut recv_task) => {
                    send_task.abort();
                }
            }
        }
    } else {
        let error = ServerMessage::Error(ErrorData {
            message: String::from("Application does not exist"),
            code: 4001,
        });

        let _ = socket
            .send(Message::Text(serde_json::to_string(&error).unwrap()))
            .await;
    }
}

enum ClientMessage {
    Subscribe(SubscribeData),
    Unsubscribe(UnsubscribeData),
    Ping(PingData),
}

impl<'de> Deserialize<'de> for ClientMessage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct ClientMessageInner {
            event: String,
            data: serde_json::Value,
        }

        let data = ClientMessageInner::deserialize(deserializer)?;

        match data.event.as_ref() {
            "pusher:subscribe" => Ok(ClientMessage::Subscribe(
                SubscribeData::deserialize(data.data).map_err(serde::de::Error::custom)?,
            )),
            "pusher:unsubscribe" => Ok(ClientMessage::Unsubscribe(
                UnsubscribeData::deserialize(data.data).map_err(serde::de::Error::custom)?,
            )),
            "pusher:ping" => Ok(ClientMessage::Ping(
                PingData::deserialize(data.data).map_err(serde::de::Error::custom)?,
            )),
            _ => Err(serde::de::Error::custom("Unknown event")),
        }
    }
}

#[derive(Deserialize)]
struct SubscribeData {
    channel: String,
    auth: Option<String>,
    channel_data: Option<String>,
}

#[derive(Deserialize)]
struct UnsubscribeData {
    channel: String,
}

#[derive(Deserialize)]
struct PingData {}

async fn handle_socket_message(
    socket_id: &Uuid,
    tx: &UnboundedSender<Message>,
    msg: Message,
    app_id: &str,
    state: &AppState,
) {
    if let Message::Text(msg) = msg {
        let msg = serde_json::from_str::<ClientMessage>(&msg);

        match msg {
            Ok(msg) => match msg {
                ClientMessage::Subscribe(msg) => {
                    let apps_lock = state.apps.read().await;
                    let channels_lock = apps_lock.get(app_id).unwrap().channels.read().await;
                    let channel = channels_lock.get(&msg.channel);

                    match channel {
                        Some(channel) => {
                            channel.add_subscriber(socket_id.clone(), tx.clone()).await;

                            let subscription_succeeded = ServerMessage::SubscriptionSucceeded(
                                msg.channel.to_string(),
                                String::from("{}"),
                            );

                            let _ = tx.send(Message::Text(
                                serde_json::to_string(&subscription_succeeded).unwrap(),
                            ));
                        }
                        None => {
                            let error = ServerMessage::SubscriptionError(
                                msg.channel.to_string(),
                                SubscriptionErrorData {
                                    error_type: String::from(""),
                                    message: String::from("Channel does not exist"),
                                    status: 400,
                                },
                            );

                            let _ = tx.send(Message::Text(serde_json::to_string(&error).unwrap()));
                        }
                    }
                }
                ClientMessage::Unsubscribe(msg) => {
                    let apps_lock = state.apps.read().await;
                    let channels_lock = apps_lock.get(app_id).unwrap().channels.read().await;
                    let channel = channels_lock.get(&msg.channel);

                    if let Some(channel) = channel {
                        channel.remove_subscriber(socket_id).await;
                    }
                }
                ClientMessage::Ping(_) => {
                    let _ = tx.send(Message::Text(
                        serde_json::to_string(&ServerMessage::Pong).unwrap(),
                    ));
                }
            },
            Err(_) => {
                let error = ServerMessage::Error(ErrorData {
                    message: String::from("Event does not exist"),
                    code: 0,
                });

                let _ = tx.send(Message::Text(serde_json::to_string(&error).unwrap()));
            }
        }
    }
}
