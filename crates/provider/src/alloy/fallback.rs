// This file is part of Rundler.
//
// Rundler is free software: you can redistribute it and/or modify it under the
// terms of the GNU Lesser General Public License as published by the Free Software
// Foundation, either version 3 of the License, or (at your option) any later version.
//
// Rundler is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
// without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
// See the GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License along with Rundler.
// If not, see https://www.gnu.org/licenses/.

use std::{
    sync::{Arc, LazyLock, Mutex, MutexGuard, PoisonError},
    task::{Context, Poll},
    time::Duration,
};

use alloy_json_rpc::{RequestPacket, ResponsePacket, RpcError};
use alloy_transport::{BoxTransport, TransportError, TransportFut};
use futures_util::FutureExt;
use metrics::Counter;
use metrics_derive::Metrics;
use regex::Regex;
use tokio::time::{Instant, timeout_at};
use tower::{Service, ServiceExt};
use tracing::warn;
use url::Url;

use super::{client_timeout_error, is_client_timeout};

/// Matches everything after the authority of a URL inside a message: the path, query and
/// fragment. Provider URLs carry API keys there (`https://lb.drpc.live/base/<key>`,
/// `https://host?apiKey=<key>`), and reqwest errors print the full URL.
static URL_SECRET_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(https?://[^/?#\s()]+)[/?#][^\s()]*").unwrap());

/// Replaces the path, query and fragment of every URL in `message` with `/…`, so that API keys
/// embedded in provider URLs do not reach the logs.
pub fn redact_url_secrets(message: &str) -> String {
    URL_SECRET_RE.replace_all(message, "$1/…").into_owned()
}

/// A transport that sends each request to an ordered list of RPC endpoints and moves on to the
/// next endpoint only when one fails at the transport level.
///
/// The first endpoint is the primary. A request goes to the first endpoint that is not cooling
/// down; if that endpoint fails with a connection error, a non-2xx HTTP response (including a
/// 429), a body that is not JSON-RPC or a client-side timeout, it is put in cooldown for
/// `cooldown` and the request moves to the next endpoint. Endpoints in cooldown are still tried,
/// after every other endpoint, so a request only fails when every endpoint has failed it. Once
/// its cooldown has passed an endpoint is preferred again, which lets the primary take traffic
/// back after it recovers.
///
/// A response that reached rundler is never retried elsewhere: that includes JSON-RPC error
/// payloads such as `execution reverted`, which another node would answer the same way.
///
/// All attempts of one request share `total_timeout`: an endpoint that is still being waited on
/// when it runs out fails the request with the client-side timeout, and no further endpoint is
/// tried. Per-endpoint timeouts still apply within it.
///
/// While the primary is in cooldown every read is served by a fallback, so fallback URLs must
/// serve the same chain and keep up with its tip.
#[derive(Clone)]
pub(crate) struct FallbackTransport {
    endpoints: Arc<[Endpoint]>,
    cooldown: Duration,
    total_timeout: Duration,
}

struct Endpoint {
    transport: BoxTransport,
    /// `scheme://host[:port]` only, so logs never contain an API key from the path or query.
    label: String,
    /// While this is in the future the endpoint is tried only after every endpoint not in
    /// cooldown.
    cooldown_until: Mutex<Option<Instant>>,
}

impl Endpoint {
    fn cooldown_until(&self) -> MutexGuard<'_, Option<Instant>> {
        // The guarded value is a plain timestamp, so a poisoned lock still holds a usable value.
        self.cooldown_until
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn in_cooldown(&self, now: Instant) -> bool {
        self.cooldown_until().is_some_and(|until| until > now)
    }

    fn start_cooldown(&self, until: Instant) {
        *self.cooldown_until() = Some(until);
    }

    fn clear_cooldown(&self) {
        *self.cooldown_until() = None;
    }
}

impl FallbackTransport {
    /// Creates a fallback transport over `endpoints`, in priority order.
    ///
    /// Panics if `endpoints` is empty.
    pub(crate) fn new(
        endpoints: Vec<(Url, BoxTransport)>,
        cooldown: Duration,
        total_timeout: Duration,
    ) -> Self {
        assert!(
            !endpoints.is_empty(),
            "fallback transport needs at least one endpoint"
        );
        let endpoints = endpoints
            .into_iter()
            .map(|(url, transport)| Endpoint {
                transport,
                label: endpoint_label(&url),
                cooldown_until: Mutex::new(None),
            })
            .collect::<Vec<_>>();
        Self {
            endpoints: endpoints.into(),
            cooldown,
            total_timeout,
        }
    }

    /// Endpoint indices in the order a request should try them: endpoints not in cooldown first,
    /// then those in cooldown, each group in configured priority order.
    fn attempt_order(&self, now: Instant) -> Vec<usize> {
        let (ready, cooling): (Vec<usize>, Vec<usize>) =
            (0..self.endpoints.len()).partition(|&i| !self.endpoints[i].in_cooldown(now));
        ready.into_iter().chain(cooling).collect()
    }

    async fn send(self, request: RequestPacket) -> Result<ResponsePacket, TransportError> {
        let metrics = FallbackMetrics::default();
        let deadline = Instant::now() + self.total_timeout;
        let order = self.attempt_order(Instant::now());
        let mut last_error = None;

        for (position, &index) in order.iter().enumerate() {
            let endpoint = &self.endpoints[index];
            let mut transport = endpoint.transport.clone();
            let attempt = async {
                let service = transport.ready().await?;
                service.call(request.clone()).await
            };

            let Ok(result) = timeout_at(deadline, attempt).await else {
                endpoint.start_cooldown(Instant::now() + self.cooldown);
                metrics.total_timeouts.increment(1);
                warn!(
                    "RPC endpoint {} did not answer within the {}s total request timeout",
                    endpoint.label,
                    self.total_timeout.as_secs(),
                );
                return Err(client_timeout_error());
            };

            match result {
                Err(error) if is_endpoint_failure(&error) => {
                    endpoint.start_cooldown(Instant::now() + self.cooldown);
                    if let Some(&next) = order.get(position + 1) {
                        metrics.endpoint_switches.increment(1);
                        warn!(
                            "RPC endpoint {} failed, trying {}: {}",
                            endpoint.label,
                            self.endpoints[next].label,
                            redact_url_secrets(&error.to_string()),
                        );
                    }
                    last_error = Some(error);
                }
                result => {
                    if result.is_ok() {
                        endpoint.clear_cooldown();
                    }
                    return result;
                }
            }
        }

        metrics.all_endpoints_failed.increment(1);
        Err(last_error.expect("fallback transport has at least one endpoint"))
    }
}

impl Service<RequestPacket> for FallbackTransport {
    type Response = ResponsePacket;
    type Error = TransportError;
    type Future = TransportFut<'static>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Readiness is awaited per endpoint in `call`, since which endpoint serves a request is
        // only decided once the request arrives.
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: RequestPacket) -> Self::Future {
        self.clone().send(request).boxed()
    }
}

/// Returns true when `error` means the endpoint did not answer the request, so the request may
/// be retried on another endpoint.
fn is_endpoint_failure(error: &TransportError) -> bool {
    match error {
        // Connection failures, non-2xx HTTP responses (429 included) and a closed backend.
        RpcError::Transport(_) => true,
        // A body that is not JSON-RPC at all, such as a proxy's HTML error page.
        RpcError::DeserError { .. } => true,
        // The client-side timeout from the per-endpoint timeout layer.
        error => is_client_timeout(error),
    }
}

fn endpoint_label(url: &Url) -> String {
    match (url.host_str(), url.port()) {
        (Some(host), Some(port)) => format!("{}://{host}:{port}", url.scheme()),
        (Some(host), None) => format!("{}://{host}", url.scheme()),
        _ => url.scheme().to_string(),
    }
}

#[derive(Metrics)]
#[metrics(scope = "provider_fallback")]
struct FallbackMetrics {
    #[metric(describe = "the count of requests moved on to the next RPC endpoint.")]
    endpoint_switches: Counter,
    #[metric(describe = "the count of requests that failed on every RPC endpoint.")]
    all_endpoints_failed: Counter,
    #[metric(describe = "the count of requests that ran out of the total request timeout.")]
    total_timeouts: Counter,
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use alloy_json_rpc::{Id, Request, Response, ResponsePayload};
    use alloy_transport::{TransportErrorKind, TransportResult};
    use futures_util::future;
    use serde_json::value::RawValue;
    use tower::util::service_fn;

    use super::*;

    const COOLDOWN: Duration = Duration::from_secs(30);
    const TOTAL_TIMEOUT: Duration = Duration::from_secs(20);

    #[derive(Clone, Copy)]
    enum Behavior {
        Answer,
        RpcErrorPayload,
        ConnectionError,
        HttpStatus(u16),
        Timeout,
        Hang,
    }

    fn request() -> RequestPacket {
        let request: Request<()> = Request::new("eth_blockNumber", Id::Number(1), ());
        RequestPacket::Single(request.serialize().unwrap())
    }

    fn answer(index: usize) -> ResponsePacket {
        let result = RawValue::from_string(format!("\"0x{index:x}\"")).unwrap();
        ResponsePacket::Single(Response {
            id: Id::Number(1),
            payload: ResponsePayload::Success(result),
        })
    }

    fn rpc_error_payload() -> ResponsePacket {
        let response: Response = serde_json::from_str(
            r#"{"jsonrpc":"2.0","id":1,"error":{"code":3,"message":"execution reverted"}}"#,
        )
        .unwrap();
        ResponsePacket::Single(response)
    }

    /// An endpoint that behaves as scripted and counts the requests it receives.
    fn endpoint(index: usize, behavior: Behavior, calls: Arc<AtomicUsize>) -> (Url, BoxTransport) {
        let url = Url::parse(&format!("https://node{index}.example/secret-key-{index}")).unwrap();
        let service = service_fn(move |_: RequestPacket| {
            calls.fetch_add(1, Ordering::SeqCst);
            async move {
                let result: TransportResult<ResponsePacket> = match behavior {
                    Behavior::Answer => Ok(answer(index)),
                    Behavior::RpcErrorPayload => Ok(rpc_error_payload()),
                    Behavior::ConnectionError => Err(TransportErrorKind::custom_str(&format!(
                        "error sending request for url (https://node{index}.example/secret-key-{index})"
                    ))),
                    Behavior::HttpStatus(status) => Err(TransportErrorKind::http_error(
                        status,
                        "bad gateway".to_string(),
                    )),
                    Behavior::Timeout => Err(client_timeout_error()),
                    Behavior::Hang => future::pending().await,
                };
                result
            }
            .boxed()
        });
        (url, BoxTransport::new(service))
    }

    fn transport(behaviors: &[Behavior]) -> (FallbackTransport, Vec<Arc<AtomicUsize>>) {
        let counters = behaviors
            .iter()
            .map(|_| Arc::new(AtomicUsize::new(0)))
            .collect::<Vec<_>>();
        let endpoints = behaviors
            .iter()
            .enumerate()
            .map(|(i, &b)| endpoint(i, b, counters[i].clone()))
            .collect();
        (
            FallbackTransport::new(endpoints, COOLDOWN, TOTAL_TIMEOUT),
            counters,
        )
    }

    fn calls(counters: &[Arc<AtomicUsize>]) -> Vec<usize> {
        counters.iter().map(|c| c.load(Ordering::SeqCst)).collect()
    }

    fn answered_by(response: &ResponsePacket) -> String {
        match response {
            ResponsePacket::Single(Response {
                payload: ResponsePayload::Success(value),
                ..
            }) => value.get().to_string(),
            other => panic!("expected a success response, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn healthy_primary_serves_every_request() {
        let (mut transport, counters) = transport(&[Behavior::Answer, Behavior::Answer]);

        for _ in 0..3 {
            let response = transport.call(request()).await.unwrap();
            assert_eq!(answered_by(&response), "\"0x0\"");
        }
        assert_eq!(calls(&counters), vec![3, 0]);
    }

    #[tokio::test]
    async fn connection_error_moves_to_the_next_endpoint() {
        let (mut transport, counters) = transport(&[Behavior::ConnectionError, Behavior::Answer]);

        let response = transport.call(request()).await.unwrap();

        assert_eq!(answered_by(&response), "\"0x1\"");
        assert_eq!(calls(&counters), vec![1, 1]);
    }

    #[tokio::test]
    async fn http_errors_and_timeouts_move_to_the_next_endpoint() {
        for failure in [
            Behavior::HttpStatus(502),
            Behavior::HttpStatus(429),
            Behavior::Timeout,
        ] {
            let (mut transport, counters) = transport(&[failure, Behavior::Answer]);

            let response = transport.call(request()).await.unwrap();

            assert_eq!(answered_by(&response), "\"0x1\"");
            assert_eq!(calls(&counters), vec![1, 1]);
        }
    }

    #[tokio::test]
    async fn rpc_error_payload_is_returned_without_trying_another_endpoint() {
        let (mut transport, counters) = transport(&[Behavior::RpcErrorPayload, Behavior::Answer]);

        let response = transport.call(request()).await.unwrap();

        assert!(response.is_error());
        assert_eq!(calls(&counters), vec![1, 0]);
    }

    #[test]
    fn only_the_typed_client_timeout_counts_as_an_endpoint_failure() {
        assert!(is_endpoint_failure(&client_timeout_error()));
        // Same text, different error: not the timeout layer's error, so no fallback.
        assert!(!is_endpoint_failure(&TransportError::local_usage_str(
            "provider request timeout from client side"
        )));
    }

    #[tokio::test]
    async fn failed_endpoint_is_skipped_while_in_cooldown() {
        let (mut transport, counters) = transport(&[Behavior::ConnectionError, Behavior::Answer]);

        transport.call(request()).await.unwrap();
        transport.call(request()).await.unwrap();
        transport.call(request()).await.unwrap();

        // The primary was tried once; the next two requests went straight to the fallback.
        assert_eq!(calls(&counters), vec![1, 3]);
    }

    #[tokio::test]
    async fn primary_is_preferred_again_after_its_cooldown() {
        let (mut transport, counters) = transport(&[Behavior::ConnectionError, Behavior::Answer]);
        transport.call(request()).await.unwrap();

        // Pretend the cooldown expired.
        transport.endpoints[0].start_cooldown(Instant::now() - Duration::from_secs(1));
        assert_eq!(transport.attempt_order(Instant::now()), vec![0, 1]);

        transport.call(request()).await.unwrap();
        assert_eq!(calls(&counters), vec![2, 2]);
    }

    #[tokio::test]
    async fn endpoints_in_cooldown_are_still_tried_last() {
        let (mut transport, counters) = transport(&[Behavior::Answer, Behavior::ConnectionError]);
        transport.endpoints[0].start_cooldown(Instant::now() + COOLDOWN);

        // The fallback fails, so the primary in cooldown answers as the last resort and leaves
        // cooldown.
        let response = transport.call(request()).await.unwrap();

        assert_eq!(answered_by(&response), "\"0x0\"");
        assert_eq!(calls(&counters), vec![1, 1]);
        assert!(!transport.endpoints[0].in_cooldown(Instant::now()));
    }

    #[tokio::test]
    async fn all_endpoints_failing_returns_the_last_error() {
        let (mut transport, counters) =
            transport(&[Behavior::ConnectionError, Behavior::HttpStatus(503)]);

        let error = transport.call(request()).await.unwrap_err();

        match error {
            RpcError::Transport(TransportErrorKind::HttpError(http)) => {
                assert_eq!(http.status, 503)
            }
            other => panic!("expected the fallback's HTTP 503, got {other:?}"),
        }
        assert_eq!(calls(&counters), vec![1, 1]);
    }

    #[tokio::test(start_paused = true)]
    async fn total_timeout_bounds_a_request_across_endpoints() {
        let (mut transport, counters) =
            transport(&[Behavior::Hang, Behavior::Answer, Behavior::Answer]);

        let started = Instant::now();
        let error = transport.call(request()).await.unwrap_err();

        // The request gave up when the shared budget ran out, without trying more endpoints.
        assert_eq!(started.elapsed(), TOTAL_TIMEOUT);
        assert!(is_client_timeout(&error));
        assert_eq!(calls(&counters), vec![1, 0, 0]);

        // The hung primary is in cooldown, so the next request is answered straight away.
        let response = transport.call(request()).await.unwrap();
        assert_eq!(answered_by(&response), "\"0x1\"");
        assert_eq!(calls(&counters), vec![1, 1, 0]);
    }

    #[test]
    fn labels_and_logs_never_contain_url_secrets() {
        let url = Url::parse("https://lb.drpc.live/base/AbCdEfSecret?x=1").unwrap();
        assert_eq!(endpoint_label(&url), "https://lb.drpc.live");
        assert_eq!(
            endpoint_label(&Url::parse("http://127.0.0.1:8545").unwrap()),
            "http://127.0.0.1:8545"
        );
        assert_eq!(
            redact_url_secrets(
                "error sending request for url (https://lb.drpc.live/base/AbCdEfSecret)"
            ),
            "error sending request for url (https://lb.drpc.live/…)"
        );
        assert_eq!(
            redact_url_secrets("failed: https://rpc.example?apiKey=AbCdEfSecret"),
            "failed: https://rpc.example/…"
        );
        assert_eq!(
            redact_url_secrets("node at http://127.0.0.1:8545 is down"),
            "node at http://127.0.0.1:8545 is down"
        );
    }
}
