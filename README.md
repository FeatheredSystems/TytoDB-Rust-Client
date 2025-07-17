# TytoDB Rust Client

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Crates.io](https://img.shields.io/crates/v/tytodb-client.svg)](https://crates.io/crates/tytodb-client)

A Rust client for the TytoDB database. This client provides a simple and easy-to-use interface for interacting with a TytoDB instance.

## Features

*   Simple and easy-to-use API
*   Support for different asynchronous runtimes (Tokio, async-std, and standard threads)
*   Connection pooling
*   Automatic reconnection

## Installation

Add the following to your `Cargo.toml` file:

```toml
[dependencies]
tytodb-client = "0.1.0"
```

By default, the client uses the `thread` feature. If you want to use a different runtime, you can specify it in the `features` section of your `Cargo.toml` file:

```toml
[dependencies]
tytodb-client = { version = "0.1.0", default-features = false, features = ["tokio"] }
```

The available features are:

*   `thread` (default)
*   `tokio`
*   `asyncstd`

## Usage

Here is a simple example of how to use the client:

```rust
use tytodb_client::{alba, client_thread, handler::{BatchBuilder, CreateContainerBuilder, CreateRowBuilder, DeleteContainerBuilder, SearchBuilder}, lo, logical_operators::LogicalOperator, ToAlbaAlbaTypes, BIGINT, MEDIUM_STRING};

use std::{fs::File, os::unix::fs::FileExt};


fn main() {
    let mut secret = [0u8;32];
    println!("--> reading the secret file");
    if let Ok(f) = File::open("secret_key_path"){
        f.read_exact_at(&mut secret,0).unwrap();
    }
    println!("\n==> secret file read succesfully");
    
    println!("--> connecting to tytodb");
    let client = client_thread::Client::connect("127.0.0.1:4287", secret).unwrap();
    println!("\n==> connected to tytodb succesfully");   
    
    let create_container_builder = CreateContainerBuilder::new()
    .put_container("nice_container".to_string())
    .insert_header("id".to_string(), BIGINT)
    .insert_header("content".to_string(), MEDIUM_STRING);
    client.execute(create_container_builder.finish().unwrap()).unwrap();
    for w in 1..100{
    let mut batched = BatchBuilder::new();
    batched = batched.transaction(true);
    
    
    let create_main_row = CreateRowBuilder::new()
    .put_container("nice_container".to_string())
    .insert_value("id".to_string(), alba!(w))
    .insert_value("content".to_string(), alba!("legal-legal-legal".to_string()));

    batched = batched.push(create_main_row);

    println!("\n~~> Batching multiple requests\n-> Create container builder\n-> Create main row");
    client.execute(batched.finish().unwrap()).unwrap();
    println!("\n\n--> Batching multiple requests finished\n=> Create container builder finished\n=> Create main row finished\n\n\n");

    let search_main_row = SearchBuilder::new()
        .add_container("nice_container".to_string())
        .add_column_name("id".to_string())
        .add_conditions( ( "id".to_string(), lo!(=), alba!(w) ) , true)
        .add_conditions( ( "content".to_string(), lo!(!=), alba!("paia-paia".to_string()) ), true)
        .add_conditions( ( "content".to_string(), lo!("&>"), alba!("legal-legal-legal".to_string()) ), true)
        .add_conditions( ( "content".to_string(), lo!("&&>"), alba!("legal-legal-legal".to_string()) ), true)
        .add_conditions( ( "content".to_string(), lo!(regex), alba!("legal-legal-legal".to_string()) ), true)
        .add_conditions( ( "id".to_string() , lo!(>), alba!(0) ), true )
        .add_conditions( ( "id".to_string() , lo!(<), alba!(w+2) ), true )
        .add_conditions( ( "id".to_string() , lo!(>=), alba!(w) ), true )
        .add_conditions( ( "id".to_string() , lo!(<=), alba!(w) ), true );

    println!("--> Search");
    let list = client.execute(search_main_row.finish().unwrap()).unwrap().row_list;
    println!("==> Search finished without errors");
    println!("=== ROW-LIST-LENGTH: {}",list.len());
    println!("\n=== LIST: {:?}",list);
    }
    let delc = DeleteContainerBuilder::new().put_container("nice_container".to_string());
    client.execute(delc.finish().unwrap()).unwrap();
}

```

## API

The client provides a simple and easy-to-use API for interacting with the database. The following methods are available:

*   `build_search()`: Builds a search query.
*   `build_edit_row()`: Builds an edit row query.
*   `build_delete_row()`: Builds a delete row query.
*   `build_delete_container()`: Builds a delete container query.
*   `build_create_row()`: Builds a create row query.
*   `build_batch_create_row()`: Builds a batch create row query.
*   `build_create_container()`: Builds a create container query.
*   `build_commit()`: Builds a commit query.
*   `build_rollback()`: Builds a rollback query.
*   `build_batch()`: Builds a batch query.

## License

This project is licensed under the Apache-2.0 license. See the [LICENSE.md](LICENSE.md) file for more details.
