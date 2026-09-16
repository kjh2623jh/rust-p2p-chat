use std::{
    io::{self, Write},
    net::TcpStream,
};

fn main() -> io::Result<()> {
    let mut stream = TcpStream::connect("127.0.0.1:9000")?;

    println!("Connected to signaling server");

    let room_code = "ABC123";

    writeln!(stream, "JOIN {room_code}")?;

    println!("Joined room: {room_code}");

    Ok(())
}
