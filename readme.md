# TytoDB Rust Client

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Crates.io](https://img.shields.io/crates/v/tytodb-client.svg)](https://crates.io/crates/tyto-client)

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
use tytodb_client::client_thread::Client;

fn main() {
    // Connect to the database
    let client = Client::connect("127.0.0.1:8080", [0; 32]).unwrap();

    // Create a new container
    let create_container_command = Client::build_create_container()
        .set_container("users".to_string())
        .add_header("name".to_string(), "string".to_string())
        .add_header("email".to_string(), "string".to_string())
        .compile();

    client.execute(create_container_command).unwrap();

    // Create a new row
    let create_row_command = Client::build_create_row()
        .set_container("users".to_string())
        .add_value("name".to_string(), "John Doe".to_string())
        .add_value("email".to_string(), "john.doe@example.com".to_string())
        .compile();

    client.execute(create_row_command).unwrap();

    // Search for the new row
    let search_command = Client::build_search()
        .set_container("users".to_string())
        .add_column_name("name".to_string())
        .add_column_name("email".to_string())
        .compile();

    let response = client.execute(search_command).unwrap();

    println!("{:?}", response);
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
