# WebSocket受信問題の独立再現コード

以下をtransport_probe.rsとして保存し、
`rustc --edition 2024 --test transport_probe.rs -o transport_probe`
および`./transport_probe --nocapture`で実行する。
2件のテストは現状の不正動作をassertする。実サービスに接続しない。
受信処理は点検時のsrc/main.rs:23049-23151から変更せず抽出した。

```rust
use std::io::{self, Read, Write, ErrorKind, Cursor};
use std::time::{Duration, Instant};
use std::thread;
const MAX_WEBSOCKET_FRAME_BYTES:u64=1024;
const APP_SERVER_RPC_READ_RETRY_INTERVAL:Duration=Duration::ZERO;
fn websocket_read_text_with_timeout<S: Read + Write>(
    stream: &mut S,
    timeout: Duration,
) -> Result<String, String> {
    let deadline = Instant::now() + timeout;
    loop {
        let mut header = [0u8; 2];
        read_exact_retry(stream, &mut header, "read websocket frame header", deadline)?;
        let opcode = header[0] & 0x0f;
        let masked = (header[1] & 0x80) != 0;
        let mut len = (header[1] & 0x7f) as u64;
        if len == 126 {
            let mut buf = [0u8; 2];
            read_exact_retry(stream, &mut buf, "read websocket frame length", deadline)?;
            len = u16::from_be_bytes(buf) as u64;
        } else if len == 127 {
            let mut buf = [0u8; 8];
            read_exact_retry(stream, &mut buf, "read websocket frame length", deadline)?;
            len = u64::from_be_bytes(buf);
        }
        if len > MAX_WEBSOCKET_FRAME_BYTES {
            return Err("websocket frame too large".to_string());
        }
        let mask = if masked {
            let mut mask = [0u8; 4];
            read_exact_retry(stream, &mut mask, "read websocket frame mask", deadline)?;
            Some(mask)
        } else {
            None
        };
        let mut payload = vec![0u8; len as usize];
        read_exact_retry(
            stream,
            &mut payload,
            "read websocket frame payload",
            deadline,
        )?;
        if let Some(mask) = mask {
            for (idx, byte) in payload.iter_mut().enumerate() {
                *byte ^= mask[idx % 4];
            }
        }

        match opcode {
            0x1 => {
                return String::from_utf8(payload)
                    .map_err(|err| format!("decode websocket text: {err}"));
            }
            0x8 => return Err("app-server websocket closed".to_string()),
            0x9 => websocket_send_pong(stream, &payload)?,
            0xA => {}
            _ => {}
        }
    }
}

fn read_exact_retry<R: Read>(
    stream: &mut R,
    mut buf: &mut [u8],
    context: &str,
    deadline: Instant,
) -> Result<(), String> {
    while !buf.is_empty() {
        match stream.read(buf) {
            Ok(0) => return Err(format!("{context}: failed to fill whole buffer")),
            Ok(nread) => {
                let tmp = buf;
                buf = &mut tmp[nread..];
            }
            Err(err)
                if matches!(
                    err.kind(),
                    ErrorKind::WouldBlock | ErrorKind::TimedOut | ErrorKind::Interrupted
                ) =>
            {
                if Instant::now() >= deadline {
                    return Err(format!("{context}: timed out waiting for app-server"));
                }
                thread::sleep(APP_SERVER_RPC_READ_RETRY_INTERVAL);
            }
            Err(err) => return Err(format!("{context}: {err}")),
        }
    }
    Ok(())
}

fn websocket_send_pong<W: Write>(stream: &mut W, payload: &[u8]) -> Result<(), String> {
    let mut frame = Vec::with_capacity(payload.len() + 6);
    frame.push(0x8A);
    frame.push(0x80 | payload.len() as u8);
    let mask = [0x70, 0x6f, 0x6e, 0x67];
    frame.extend_from_slice(&mask);
    frame.extend(
        payload
            .iter()
            .enumerate()
            .map(|(idx, byte)| byte ^ mask[idx % 4]),
    );
    stream
        .write_all(&frame)
        .map_err(|err| format!("write websocket pong: {err}"))
}
struct InterruptedFrame { step:u8, rest:Cursor<Vec<u8>> }
impl Read for InterruptedFrame {
 fn read(&mut self, b:&mut [u8])->io::Result<usize>{
  match self.step {
   0=>{self.step=1;b[0]=0x81;Ok(1)},
   1=>{self.step=2;Err(io::Error::new(ErrorKind::TimedOut,"injected pause"))},
   _=>self.rest.read(b)
  }
 }
}
impl Write for InterruptedFrame {
 fn write(&mut self,b:&[u8])->io::Result<usize>{Ok(b.len())}
 fn flush(&mut self)->io::Result<()>{Ok(())}
}
#[test]
fn partial_header_timeout_loses_frame_boundary(){
 let mut s=InterruptedFrame{step:0,rest:Cursor::new(vec![2,b'{',b'}'])};
 let e=websocket_read_text_with_timeout(&mut s,Duration::ZERO).unwrap_err();
 assert!(e.contains("timed out"));
 let e=websocket_read_text_with_timeout(&mut s,Duration::from_secs(1)).unwrap_err();
 assert!(e.contains("failed to fill whole buffer"),"{e}");
}
#[test]
fn fragmented_json_is_returned_before_final_frame(){
 let mut s=Cursor::new(vec![0x01,1,b'{',0x80,1,b'}']);
 let result=websocket_read_text_with_timeout(&mut s,Duration::from_secs(1)).unwrap();
 assert_eq!(result,"{");
 assert_eq!(s.position(),3);
}


```
