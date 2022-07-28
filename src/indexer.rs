use anyhow::{anyhow, Result};
use ethers::core::types::{Block, Transaction};
use ethers::prelude::*;
use ethers::providers::{Http, Middleware, Provider};
use futures::stream::FuturesUnordered;
use log::{error, info};
use rayon::prelude::*;
use sqlx::pool::Pool;
use sqlx::types::BigDecimal;
use sqlx::Postgres;
use std::str::FromStr;
use std::sync::Arc;

pub async fn transaction_from_block(
    provider: Arc<Provider<Http>>,
    block_id: u64,
) -> Result<Block<Transaction>> {
    let block = provider.get_block_with_txs(block_id).await?;
    match block {
        Some(block) => Ok(block),
        None => Err(anyhow!("Not a valid block")),
    }
}

pub async fn insert_transactions_from_block(
    provider: Arc<Provider<Http>>,
    block: Block<Transaction>,
    db: Arc<Pool<Postgres>>,
) -> Result<()> {
    let block_id = block.number.unwrap();
    let transactions = &block.transactions;
    let time = vec![block.time().unwrap().timestamp() as i32; transactions.len()];

    let from: Vec<String> = transactions
        .par_iter()
        .map(|transaction| format!("{:?}", transaction.from))
        .collect();

    let to: Vec<String> = transactions
        .par_iter()
        .map(|transaction| {
            transaction
                .to
                .map_or_else(|| String::new(), |t| format!("{:?}", t))
        })
        .collect();

    let value: Vec<BigDecimal> = transactions
        .par_iter()
        .map(|transaction| u256_decimal(transaction.value).unwrap())
        .collect();

    let gas: Vec<BigDecimal> = transactions
        .par_iter()
        .map(|transaction| u256_decimal(transaction.gas).unwrap())
        .collect();

    let gas_price: Vec<BigDecimal> = transactions
        .par_iter()
        .map(|transaction| {
            transaction.gas_price.map_or_else(
                || BigDecimal::from(0),
                |price| u256_decimal(price).unwrap_or_else(|_| BigDecimal::from(0)),
            )
        })
        .collect::<Vec<BigDecimal>>();

    let tx_hash: Vec<String> = transactions
        .par_iter()
        .map(|transaction| format!("{:?}", transaction.hash))
        .collect();

    let contract_to = vec![String::new(); transactions.len()];
    let contract_value = vec![String::new(); transactions.len()];

    let block_id_big = vec![u64_decimal(block_id).unwrap(); transactions.len()];
    let status = vec![true; transactions.len()];

    sqlx::query!(
        r#"INSERT INTO public.ethtxs(time, txfrom, txto, value, gas, gasprice, block, txhash, contract_to, contract_value, status)
          SELECT * FROM UNNEST($1::INTEGER[], $2::TEXT[]::CITEXT[], $3::TEXT[]::CITEXT[], $4::NUMERIC[], $5::NUMERIC[], $6::NUMERIC[], $7::NUMERIC[], $8::TEXT[]::CITEXT[], $9::TEXT[]::CITEXT[], $10::TEXT[]::CITEXT[], $11::BOOL[])"#,
       &time[..],
       &from[..],
       &to[..],
       &value[..],
       &gas[..],
       &gas_price[..],
       &block_id_big[..],
       &tx_hash[..],
       &contract_to[..],
       &contract_value[..],
       &status[..]
    ).execute(&*db).await
    ?;

    Ok(())
}

pub async fn insert_transaction_from_block(
    provider: Arc<Provider<Http>>,
    time: i32,
    block_id: U64,
    transaction: &Transaction,
    db: Arc<Pool<Postgres>>,
) -> Result<()> {
    //let is_contract_transfer ;
    let from = format!("{:?}", transaction.from);
    let to = transaction
        .to
        .map_or_else(|| String::new(), |t| format!("{:?}", t));

    info!("Processing transaction : {}", block_id);

    let value = u256_decimal(transaction.value)?;
    let gas = u256_decimal(transaction.gas)?;
    let gas_price: Option<BigDecimal> = transaction
        .gas_price
        .and_then(|price| u256_decimal(price).ok());
    let tx_hash = format!("{:?}", transaction.hash);

    let contract_to = String::new();
    let contract_value = String::new();

    let status: bool = provider
        .get_transaction_receipt(transaction.hash)
        .await
        .ok()
        .flatten()
        .and_then(|receipt| receipt.status)
        .is_some();

    let block_id_big = u64_decimal(block_id)?;

    sqlx::query!(
        r#"INSERT INTO public.ethtxs(time, txfrom, txto, value, gas, gasprice, block, txhash, contract_to, contract_value, status)
           VALUES ($1, $2::TEXT::CITEXT, $3::TEXT::CITEXT, $4, $5, $6, $7, $8::TEXT::CITEXT, $9::TEXT::CITEXT, $10::TEXT::CITEXT, $11)"#,
        time,
        from,
        to,
        value,
        gas,
        gas_price,
        block_id_big,
        tx_hash,
        contract_to,
        contract_value,
        status
    ).execute(&*db).await
    ?;

    Ok(())
}

fn encode_transaction(transaction: &Transaction) {}

fn u256_decimal(src: U256) -> Result<BigDecimal> {
    let b = BigDecimal::from_str(&src.to_string())?;
    Ok(b)
}

fn u64_decimal(src: U64) -> Result<BigDecimal> {
    let b = BigDecimal::from_str(&src.to_string())?;
    Ok(b)
}
