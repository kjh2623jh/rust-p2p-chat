use std::{io, net::TcpListener};

fn main() -> io::Result<()> {
    let listener = TcpListener::bind("0.0.0.0:9000")?;

    println!("Signaling server listening on 0.0.0.0:9000");

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let addr = stream.peer_addr()?;

                println!("Client connected: {addr}");
            }

            Err(error) => {
                eprintln!("Connection error: {error}");
            }
        }
    }

    Ok(())
}
