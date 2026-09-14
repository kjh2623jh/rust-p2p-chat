use std::{io, net::TcpStream};

fn main() -> io::Result<()> {
    let stream = TcpStream::connect("127.0.0.1:9000")?;

    println!("Connected to signaling server");
    println!("Server: {}", stream.peer_addr()?);

    Ok(())
}
