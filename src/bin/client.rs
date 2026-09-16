use tokio::{io::AsyncWriteExt, net::TcpStream};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let mut stream = TcpStream::connect("127.0.0.1:9000").await?;

    println!("Connected to signaling server");

    let room_code = "ABC123";

    let message = format!("JOIN {room_code}\n");

    stream.write_all(message.as_bytes()).await?;

    println!("Joined room: {room_code}");

    Ok(())
}
