use anyhow::{anyhow, Result};
use ethers::core::types::{Block, Transaction};
use ethers::prelude::*;
use ethers::providers::{Http, Middleware, Provider};
use log::{error, info};
use sqlx::pool::{Pool, PoolConnection};
use sqlx::types::BigDecimal;
use sqlx::{Executor, Postgres};
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
) {
    let time = block.time().unwrap().timestamp() as i32;
    let block_id = block.number.unwrap();
    let transactions = block.transactions;
    info!("Processing block : {}", block_id);

    for transaction in transactions.iter() {
        insert_transaction_from_block(provider.clone(), time, block_id, transaction, db.clone())
            .await
            .map_err(|e| {
                error!(
                    "Failed to insert transaction on block {}  with transaction hash {} with error {}",
                    block_id, transaction.hash, e
                );
            });
    }
}

pub async fn insert_transaction_from_block(
    provider: Arc<Provider<Http>>,
    time: i32,
    block_id: U64,
    transaction: &Transaction,
    db: Arc<Pool<Postgres>>,
) -> Result<()> {
    info!("Processing transaction : {}", block_id);
    //let is_contract_transfer ;
    let from = transaction.from.to_string();
    let to = transaction
        .to
        .map_or_else(|| String::new(), |t| t.to_string());

    let diogo = Address::from_str("0x4435e31fB15844436390F3c9819e32976edD0564")?;

    if transaction.from != diogo && transaction.to != Some(diogo) {
        return Ok(());
    };

    let value = u256_decimal(transaction.value)?;
    let gas = u256_decimal(transaction.gas)?;
    let gas_price: Option<BigDecimal> = transaction
        .gas_price
        .and_then(|price| u256_decimal(price).ok());
    let tx_hash = transaction.hash.to_string();

    //let contract_to = transaction.input[10..-64];
    //let contract_value= transaction.input[74..];

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

    info!("inserting transaction : time {} from {} to {} value {} gas {} gas_price {:#?} block_id_big {} tx_hash {} contract_to {} contract_value {} status {} ", 
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
 );

    let sql = sqlx::query!(
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

fn u256_decimal(src: U256) -> Result<BigDecimal> {
    let b = BigDecimal::from_str(&src.to_string())?;
    Ok(b)
}

fn u64_decimal(src: U64) -> Result<BigDecimal> {
    let b = BigDecimal::from_str(&src.to_string())?;
    Ok(b)
}
