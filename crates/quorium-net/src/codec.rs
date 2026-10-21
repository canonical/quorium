use std::fmt;

use serde::{Deserialize, Serialize};

pub const FORMAT_VERSION: u16 = 1;
pub const MAX_FRAME: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodecError {
    UnsupportedFormat(u16),
    Malformed,
    FrameTooLarge(usize),
}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CodecError::UnsupportedFormat(v) => write!(f, "unsupported wire format version {v}"),
            CodecError::Malformed => f.write_str("malformed message"),
            CodecError::FrameTooLarge(n) => write!(f, "frame of {n} bytes exceeds the limit"),
        }
    }
}

impl std::error::Error for CodecError {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Envelope {
    pub format: u16,
    pub type_name: String,
    pub method: u32,
    pub state_version: u32,
    #[serde(with = "serde_bytes")]
    pub payload: Vec<u8>,
}

impl Envelope {
    pub fn encode(&self) -> Vec<u8> {
        postcard::to_allocvec(self).expect("envelopes always serialize")
    }

    pub fn decode(bytes: &[u8]) -> Result<Envelope, CodecError> {
        let format = peek_format(bytes)?;
        if format != FORMAT_VERSION {
            return Err(CodecError::UnsupportedFormat(format));
        }
        match postcard::take_from_bytes::<Envelope>(bytes) {
            Ok((e, [])) => Ok(e),
            _ => Err(CodecError::Malformed),
        }
    }
}

fn peek_format(bytes: &[u8]) -> Result<u16, CodecError> {
    postcard::take_from_bytes::<u16>(bytes).map(|(v, _)| v).map_err(|_| CodecError::Malformed)
}

pub fn encode_frame(body: &[u8]) -> Result<Vec<u8>, CodecError> {
    if body.len() > MAX_FRAME {
        return Err(CodecError::FrameTooLarge(body.len()));
    }
    let mut out = Vec::with_capacity(4 + body.len());
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(body);
    Ok(out)
}

#[derive(Debug, Default)]
pub struct FrameDecoder {
    buf: Vec<u8>,
    start: usize,
}

impl FrameDecoder {
    pub fn push(&mut self, bytes: &[u8]) {
        if self.start > 0 && self.start == self.buf.len() {
            self.buf.clear();
            self.start = 0;
        }
        self.buf.extend_from_slice(bytes);
    }

    pub fn next_frame(&mut self) -> Result<Option<Vec<u8>>, CodecError> {
        let available = &self.buf[self.start..];
        if available.len() < 4 {
            return Ok(None);
        }
        let len = u32::from_le_bytes(available[..4].try_into().expect("four bytes")) as usize;
        if len > MAX_FRAME {
            return Err(CodecError::FrameTooLarge(len));
        }
        if available.len() < 4 + len {
            return Ok(None);
        }
        let frame = available[4..4 + len].to_vec();
        self.start += 4 + len;
        if self.start > 64 * 1024 && self.start * 2 > self.buf.len() {
            self.buf.drain(..self.start);
            self.start = 0;
        }
        Ok(Some(frame))
    }
}
