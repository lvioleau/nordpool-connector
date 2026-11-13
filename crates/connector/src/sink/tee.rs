use chrono::{DateTime, Utc};

use super::FrameSink;
use crate::proto::pmd::server_frame::Payload as ServerPayload;

/// Fans every frame out to a list of inner sinks in order. Stops on the first
/// error — the reconnect wrapper handles it like any other sink failure.
pub struct TeeSink {
    sinks: Vec<Box<dyn FrameSink>>,
}

impl TeeSink {
    pub fn new(sinks: Vec<Box<dyn FrameSink>>) -> Self {
        Self { sinks }
    }
}

impl FrameSink for TeeSink {
    fn handle(
        &mut self,
        payload: &ServerPayload,
        bytes: &[u8],
        received_at: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        for sink in &mut self.sinks {
            sink.handle(payload, bytes, received_at)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::pmd::Heartbeat;
    use std::sync::{Arc, Mutex};

    fn any_payload() -> ServerPayload {
        ServerPayload::Heartbeat(Heartbeat {})
    }

    /// Fixed receive time for the many tests where the value itself is irrelevant.
    fn received_at() -> DateTime<Utc> {
        DateTime::UNIX_EPOCH
    }

    #[derive(Clone, Default)]
    struct CountingSink {
        received: Arc<Mutex<Vec<Vec<u8>>>>,
    }
    impl FrameSink for CountingSink {
        fn handle(
            &mut self,
            _payload: &ServerPayload,
            bytes: &[u8],
            _recv_time: DateTime<Utc>,
        ) -> anyhow::Result<()> {
            self.received.lock().unwrap().push(bytes.to_vec());
            Ok(())
        }
    }

    #[derive(Default)]
    struct ErroringSink;
    impl FrameSink for ErroringSink {
        fn handle(
            &mut self,
            _payload: &ServerPayload,
            _bytes: &[u8],
            _recv_time: DateTime<Utc>,
        ) -> anyhow::Result<()> {
            Err(anyhow::anyhow!("kaboom"))
        }
    }

    #[test]
    fn tee_sink_fans_out_to_every_inner_sink() {
        // Given a tee wrapping two independent recording sinks, each sharing its received-bytes
        // log with a clone the test holds onto so it can inspect what each one saw.
        let a = CountingSink::default();
        let b = CountingSink::default();
        let mut tee = TeeSink::new(vec![Box::new(a.clone()), Box::new(b.clone())]);

        // When two frames are handled — the driver's fan-out of each received frame to every sink.
        tee.handle(&any_payload(), b"hello", received_at()).unwrap();
        tee.handle(&any_payload(), b"world", received_at()).unwrap();

        // Then both sinks saw both frames (the tee duplicated, not split, the stream) and the
        // bytes arrived intact and in order.
        assert_eq!(a.received.lock().unwrap().len(), 2);
        assert_eq!(b.received.lock().unwrap().len(), 2);
        assert_eq!(b.received.lock().unwrap()[0], b"hello");
    }

    #[test]
    fn tee_sink_stops_at_first_error_and_propagates_it() {
        // Given a tee of three sinks in order: a recording one, one that always errors, then a
        // second recording one. The third sink must NOT receive the bytes because the second one
        // errored — fan-out is sequential and short-circuiting, not best-effort.
        let a = CountingSink::default();
        let c = CountingSink::default();
        let mut tee = TeeSink::new(vec![
            Box::new(a.clone()),
            Box::new(ErroringSink),
            Box::new(c.clone()),
        ]);

        // When a frame is handled — the middle sink fails partway through the fan-out.
        let result = tee.handle(&any_payload(), b"frame", received_at());

        // Then the error propagates to the caller (so the reconnect wrapper treats it like any
        // sink failure), the first sink already got the frame, and the third never did because the
        // tee stopped at the first error.
        assert!(result.is_err());
        assert_eq!(a.received.lock().unwrap().len(), 1);
        assert_eq!(c.received.lock().unwrap().len(), 0);
    }
}
