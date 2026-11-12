//! Captures WARN-level `tracing` output for tests that pin a log line.

use std::io::Write;
use std::sync::{Arc, Mutex};

#[derive(Clone, Default)]
struct CaptureWriter(Arc<Mutex<Vec<u8>>>);

impl Write for CaptureWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .expect("capture lock poisoned")
            .extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CaptureWriter {
    type Writer = CaptureWriter;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

struct DynamicWarnInterest;

impl tracing::Subscriber for DynamicWarnInterest {
    fn register_callsite(
        &self,
        meta: &'static tracing::Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        if *meta.level() <= tracing::Level::WARN {
            tracing::subscriber::Interest::sometimes()
        } else {
            tracing::subscriber::Interest::never()
        }
    }
    fn max_level_hint(&self) -> Option<tracing::level_filters::LevelFilter> {
        Some(tracing::level_filters::LevelFilter::WARN)
    }
    fn enabled(&self, _meta: &tracing::Metadata<'_>) -> bool {
        false
    }
    fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}
    fn event(&self, _event: &tracing::Event<'_>) {}
    fn enter(&self, _span: &tracing::span::Id) {}
    fn exit(&self, _span: &tracing::span::Id) {}
}

pub(crate) fn capture_logs(f: impl FnOnce()) -> String {
    static DYNAMIC_INTEREST: std::sync::Once = std::sync::Once::new();
    DYNAMIC_INTEREST.call_once(|| {
        let _ = tracing::subscriber::set_global_default(DynamicWarnInterest);
    });

    let buf = Arc::new(Mutex::new(Vec::new()));
    let writer = CaptureWriter(Arc::clone(&buf));
    let subscriber = tracing_subscriber::fmt()
        .with_writer(writer)
        .with_max_level(tracing::Level::WARN)
        .with_ansi(false)
        .finish();
    tracing::subscriber::with_default(subscriber, || {
        tracing::callsite::rebuild_interest_cache();
        f();
    });
    let bytes = buf.lock().expect("capture lock poisoned").clone();
    String::from_utf8_lossy(&bytes).into_owned()
}
