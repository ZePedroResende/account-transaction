// We prefer to keep `main.rs` and `lib.rs` separate as it makes it easier to add extra helper
// binaries later which share code with the main project. It could save you from a nontrivial
// refactoring effort in the future.
//
// Whether to make `main.rs` just a thin shim that awaits a `run()` function in `lib.rs`, or
// to put the application bootstrap logic here is an open question. Both approaches have their
// upsides and their downsides. Your input is welcome!

use anyhow::Context;
use clap::Parser;
use sqlx::postgres::PgPoolOptions;

use log::{error, info};
use realworld_axum_sqlx::config::Config;
use realworld_axum_sqlx::indexer::{self, insert_transactions_from_block};

use ethers::prelude::*;
use ethers::providers::{Authorization, Http, Provider};
use futures::{StreamExt, TryFutureExt};
use indicatif::ProgressBar;
use std::sync::Arc;
use url::Url;

use futures::stream::FuturesUnordered;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // This returns an error if the `.env` file doesn't exist, but that's not what we want
    // since we're not going to use a `.env` file if we deploy this application.
    dotenv::dotenv().ok();

    // Initialize the logger.
    env_logger::init();

    // Parse our configuration from the environment.
    // This will exit with a help message if something is wrong.
    let config = Config::parse();

    // We create a single connection pool for SQLx that's shared across the whole application.
    // This saves us from opening a new connection for every API call, which is wasteful.
    let db = PgPoolOptions::new()
        // The default connection limit for a Postgres server is 100 connections, minus 3 for superusers.
        // Since we're using the default superuser we don't have to worry about this too much,
        // although we should leave some connections available for manual access.
        //
        // If you're deploying your application with multiple replicas, then the total
        // across all replicas should not exceed the Postgres connection limit.
        .max_connections(50)
        .connect(&config.database_url)
        .await
        .context("could not connect to database_url")?;

    // This embeds database migrations in the application binary so we can ensure the database
    // is migrated correctly on startup
    sqlx::migrate!().run(&db).await?;

    info!("Connecting to provider...");

    let http: Http = Http::new_with_auth(
        Url::parse(&config.provider_url).unwrap(),
        Authorization::basic(config.provider_username, config.provider_password),
    )?;

    let provider: Provider<Http> = Provider::new(http);

    let provider = Arc::new(provider);

    info!("Connected to provider !");

    //    let mut stream = provider.watch_blocks().await?;

    //    while let Some(block) = stream.next().await {
    //        let block = provider.get_block(block).await?.unwrap();
    //        println!(
    //            "Ts: {:?}, block number: {} -> {:?}",
    //            block.timestamp,
    //            block.number.unwrap(),
    //            block.hash.unwrap()
    //        );
    //    }

    //let start_block = 13217541;
    let start_block = 13380000;

    let mut current_block = start_block;

    let mut last_block = provider
        .get_block(BlockNumber::Latest)
        .await?
        .unwrap()
        .number
        .unwrap()
        .as_u64();

    let db_arc = Arc::new(db);

    //   while current_block < last_block {
    //       tokio::spawn(async {
    //           let block = indexer::transaction_from_block(provider.clone(), current_block.clone())
    //               .await
    //               .unwrap();
    //           insert_transactions_from_block(provider.clone(), block, db_arc.clone()).await;
    //       });

    //       //   last_block = provider
    //       //       .get_block(BlockNumber::Latest)
    //       //       .await?
    //       //       .unwrap()
    //       //       .number
    //       //       .unwrap()
    //       //       .as_u64();

    //       current_block. += 1;
    //   }
    //    let pb = ProgressBar::new(last_block - current_block);
    //
    //    stream::iter(current_block..last_block)
    //        .map(|block_id| {
    //            let p = provider.clone();
    //            let d = db_arc.clone();
    //            let pb = pb.clone();
    //            async move {
    //                let block = indexer::transaction_from_block(p.clone(), block_id)
    //                    .await
    //                    .unwrap();
    //                insert_transactions_from_block(p.clone(), block, d).await;
    //                pb.inc(1);
    //                Ok(())
    //            }
    //        })
    //        .buffer_unordered(60)
    //        .collect::<Vec<Result<()>>>()
    //        .await;

    let pb = ProgressBar::new(last_block - current_block);

    let mut tasks = FuturesUnordered::new();

    (current_block..last_block)
        .into_iter()
        .for_each(|block_id| {
            let p = provider.clone();
            let d = db_arc.clone();
            let pb = pb.clone();
            tasks.push(tokio::spawn(async move {
                indexer::transaction_from_block(p.clone(), block_id)
                    .and_then(move |block| insert_transactions_from_block(p.clone(), block, d))
                    .await
            }));
        });

    while let Some(item) = tasks.next().await {
        item.map_err(|e| {
            error!("Failed to with error {}", e);
        });
        pb.inc(1);
    }

    info!("synced !");
    info!("Formed a stream");
    //    let mut stream = provider.watch_blocks().await?;
    //    while let Some(block) = stream.next().await {
    //        let block = provider.get_block(block).await?.unwrap();
    //        println!(
    //            "Ts: {:?}, block number: {} -> {:?}",
    //            block.timestamp,
    //            block.number.unwrap(),
    //            block.hash.unwrap()
    //        );
    //    }

    // Finally, we spin up our API.

    Ok(())
}
