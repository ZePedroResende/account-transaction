// We prefer to keep `main.rs` and `lib.rs` separate as it makes it easier to add extra helper
// binaries later which share code with the main project. It could save you from a nontrivial
// refactoring effort in the future.
//
// Whether to make `main.rs` just a thin shim that awaits a `run()` function in `lib.rs`, or
// to put the application bootstrap logic here is an open question. Both approaches have their
// upsides and their downsides. Your input is welcome!

use anyhow::{Context, Result};
use clap::Parser;
use sqlx::postgres::PgPoolOptions;

use log::{error, info};
use realworld_axum_sqlx::config::Config;
use realworld_axum_sqlx::indexer::{self, insert_transactions_from_block};

use ethers::prelude::*;
use ethers::providers::{Authorization, Http, Provider};
use futures::stream;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use std::sync::Arc;
use url::Url;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();

    env_logger::init();

    let config = Config::parse();

    let db = PgPoolOptions::new()
        .max_connections(90)
        .connect(&config.database_url)
        .await
        .context("could not connect to database_url")?;

    sqlx::migrate!().run(&db).await?;

    info!("Connecting to provider...");

    let http: Http = Http::new_with_auth(
        Url::parse(&config.provider_url).unwrap(),
        Authorization::basic(config.provider_username, config.provider_password),
    )?;

    let retry_client: RetryClient<Http> =
        RetryClient::new(http, Box::new(HttpRateLimitRetryPolicy), 1000, 10);

    let provider: Provider<RetryClient<Http>> = Provider::<RetryClient<Http>>::new(retry_client);

    let provider = Arc::new(provider);

    info!("Connected to provider !");

    let start_block = 13217541;

    let current_block = start_block;

    let last_block = provider
        .get_block(BlockNumber::Latest)
        .await?
        .context("Cant get block")?
        .number
        .context("Cant get block number")?
        .as_u64();

    let db_arc = Arc::new(db);

    let pb = ProgressBar::new(last_block - current_block);

    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{wide_bar:.cyan/blue}] {pos}/{len} ({eta})")
            .progress_chars("#>-"),
    );

    let mut tasks = FuturesUnordered::new();

    let stream = stream::iter(current_block..last_block)
        .map(|block_id| {
            let d = db_arc.clone();
            let tasks = &tasks;
            let provider = provider.clone();
            async move {
                let block = indexer::transaction_from_block(provider, block_id).await?;
                tasks.push(tokio::spawn(async move {
                    insert_transactions_from_block(block, d).await
                }));
                Ok(())
            }
        })
        .buffer_unordered(1)
        .collect::<Vec<Result<()>>>()
        .await;

    let mut errors = Vec::new();

    while let Some(item) = tasks.next().await {
        match item {
            Ok(result) => match result {
                Ok(_) => pb.inc(1),
                Err(e) => {
                    error!("{}", e);
                    errors.push(e);
                }
            },
            Err(e) => info!("{}", e),
        }
    }

    for item in stream.into_iter() {
        match item {
            Ok(_) => pb.inc(1),
            Err(e) => info!("{}", e),
        }
    }

    println!("error length {}", errors.len());

    info!("synced !");
    println!("synced !");

    Ok(())
}
