//! Size-limited RESP2. Each reader has a single owner for cancellation safety.
use crate::RedisError;
use std::{future::Future, pin::Pin};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
};

#[derive(Debug, PartialEq)]
pub(crate) enum Resp {
    Simple(Vec<u8>),
    Error(Vec<u8>),
    Integer(i64),
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<Resp>>),
}
pub(crate) struct Connection {
    reader: BufReader<TcpStream>,
    limit: usize,
}
impl Connection {
    pub fn new(stream: TcpStream, limit: usize) -> Self {
        Self {
            reader: BufReader::new(stream),
            limit,
        }
    }
    pub async fn write(&mut self, args: &[&[u8]]) -> Result<(), RedisError> {
        let mut buffer = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            buffer.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
            buffer.extend_from_slice(arg);
            buffer.extend_from_slice(b"\r\n");
        }
        self.reader.get_mut().write_all(&buffer).await?;
        self.reader.get_mut().flush().await?;
        Ok(())
    }
    pub async fn read(&mut self) -> Result<Resp, RedisError> {
        let mut remaining = self.limit;
        self.read_inner(0, &mut remaining).await
    }
    fn read_inner<'a>(
        &'a mut self,
        depth: usize,
        remaining: &'a mut usize,
    ) -> Pin<Box<dyn Future<Output = Result<Resp, RedisError>> + Send + 'a>> {
        Box::pin(async move {
            if depth > 16 {
                return Err(RedisError::Protocol("RESP nesting limit".into()));
            }
            let prefix = self.reader.read_u8().await?;
            let line = self.line(remaining).await?;
            let number = || {
                std::str::from_utf8(&line)
                    .ok()
                    .and_then(|v| v.parse::<i64>().ok())
                    .ok_or_else(|| RedisError::Protocol("invalid RESP integer".into()))
            };
            match prefix {
                b'+' => Ok(Resp::Simple(line)),
                b'-' => Ok(Resp::Error(line)),
                b':' => Ok(Resp::Integer(number()?)),
                b'$' => {
                    let length = number()?;
                    if length == -1 {
                        return Ok(Resp::Bulk(None));
                    }
                    let length = usize::try_from(length)
                        .map_err(|_| RedisError::Protocol("invalid bulk length".into()))?;
                    if length > *remaining {
                        return Err(RedisError::Protocol("RESP size limit".into()));
                    }
                    *remaining -= length;
                    let mut data = vec![0; length];
                    self.reader.read_exact(&mut data).await?;
                    let mut ending = [0; 2];
                    self.reader.read_exact(&mut ending).await?;
                    if ending != *b"\r\n" {
                        return Err(RedisError::Protocol("invalid RESP terminator".into()));
                    }
                    Ok(Resp::Bulk(Some(data)))
                }
                b'*' => {
                    let length = number()?;
                    if length == -1 {
                        return Ok(Resp::Array(None));
                    }
                    let length = usize::try_from(length)
                        .map_err(|_| RedisError::Protocol("invalid array length".into()))?;
                    if length > 1024 || length > *remaining {
                        return Err(RedisError::Protocol("RESP array limit".into()));
                    }
                    let mut values = Vec::with_capacity(length);
                    for _ in 0..length {
                        values.push(self.read_inner(depth + 1, remaining).await?);
                    }
                    Ok(Resp::Array(Some(values)))
                }
                _ => Err(RedisError::Protocol("unsupported RESP prefix".into())),
            }
        })
    }
    async fn line(&mut self, remaining: &mut usize) -> Result<Vec<u8>, RedisError> {
        let mut result = vec![];
        loop {
            let buffer = self.reader.fill_buf().await?;
            if buffer.is_empty() {
                return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof).into());
            }
            let take = buffer
                .iter()
                .position(|&byte| byte == b'\n')
                .map_or(buffer.len(), |position| position + 1);
            if take > *remaining || result.len() + take > 4096 {
                return Err(RedisError::Protocol("RESP line limit".into()));
            }
            result.extend_from_slice(&buffer[..take]);
            *remaining -= take;
            self.reader.consume(take);
            if result.ends_with(b"\n") {
                break;
            }
        }
        if !result.ends_with(b"\r\n") {
            return Err(RedisError::Protocol("invalid RESP line ending".into()));
        }
        result.truncate(result.len() - 2);
        Ok(result)
    }
    pub async fn command(&mut self, args: &[&[u8]]) -> Result<Resp, RedisError> {
        self.write(args).await?;
        match self.read().await? {
            Resp::Error(error) => Err(RedisError::Server(
                String::from_utf8_lossy(&error).into_owned(),
            )),
            response => Ok(response),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        net::TcpListener,
        time::{timeout, Duration},
    };

    async fn parse(bytes: Vec<u8>, limit: usize) -> Result<Resp, RedisError> {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            for chunk in bytes.chunks(3) {
                if socket.write_all(chunk).await.is_err() {
                    return;
                }
                tokio::task::yield_now().await;
            }
        });
        let mut connection = Connection::new(TcpStream::connect(address).await.unwrap(), limit);
        let result = timeout(Duration::from_secs(1), connection.read())
            .await
            .unwrap();
        server.await.unwrap();
        result
    }
    #[tokio::test]
    async fn fragmented_binary_bulk_and_nested_arrays() {
        assert_eq!(
            parse(b"*3\r\n$4\r\n\x00\xff\r\n\r\n:-42\r\n*-1\r\n".to_vec(), 128)
                .await
                .unwrap(),
            Resp::Array(Some(vec![
                Resp::Bulk(Some(b"\x00\xff\r\n".to_vec())),
                Resp::Integer(-42),
                Resp::Array(None)
            ]))
        );
    }
    #[tokio::test]
    async fn malformed_lengths_terminators_and_large_arrays_fail() {
        for bytes in [
            b"$-2\r\n".as_slice(),
            b"$99999999\r\n",
            b"$2\r\nhiXX",
            b"*99999999\r\n",
            b"+hello\n",
            b":nonnumeric\r\n",
        ] {
            assert!(matches!(
                parse(bytes.to_vec(), 32).await,
                Err(RedisError::Protocol(_))
            ));
        }
    }
    #[tokio::test]
    async fn deep_and_unbounded_line_inputs_fail() {
        let mut nested = b"*1\r\n".repeat(18);
        nested.extend_from_slice(b"+OK\r\n");
        assert!(matches!(
            parse(nested, 1024).await,
            Err(RedisError::Protocol(_))
        ));
        let mut line = vec![b'+'; 5000];
        line.extend_from_slice(b"\r\n");
        assert!(matches!(
            parse(line, 8192).await,
            Err(RedisError::Protocol(_))
        ));
    }
}
