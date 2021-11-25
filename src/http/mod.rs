use crate::config::Config;
use anyhow::Context;
use axum::{AddExtensionLayer, Router};
use sqlx::PgPool;
use std::sync::Arc;
use tower::ServiceBuilder;

mod error;
mod extractor;
mod profiles;
mod users;

pub use error::Error;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone)]
struct ApiContext {
    config: Arc<Config>,
    db: PgPool,
}

pub async fn serve(config: Config, db: PgPool) -> anyhow::Result<()> {
    let app = api_router().layer(
        ServiceBuilder::new().layer(AddExtensionLayer::new(ApiContext {
            // In other projects I've passed this stuff as separate objects, e.g.
            // using a separate actix-web `Data` extractor for each of `Config`, `PgPool`, etc.
            // It just ends up being kind of annoying that way, but does have the whole
            // "pass only what you need where you need it" angle.
            //
            // It may not be a bad idea if you need your API to be more modular (turn routes
            // on and off, and disable any unused extension objects) but it's really up to a
            // judgement call.
            config: Arc::new(config),
            db,
        })),
    );

    // We use 8080 as our default HTTP server port, it's pretty easy to remember.
    //
    // Note that any port below 1024 needs superuser privileges to bind on Linux,
    // so 80 isn't usually used as a default for that reason.
    axum::Server::bind(&"0.0.0.0:8080".parse()?)
        .serve(app.into_make_service())
        .await
        .context("error running HTTP server")
}

fn api_router() -> Router {
    users::router().merge(profiles::router())
}
