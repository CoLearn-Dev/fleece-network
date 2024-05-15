use std::{str::FromStr, thread};

use bytes::Bytes;
use crossbeam_channel::bounded;
use fleece_network::{
    channel::InboundHandle,
    peer::{codec, eventloop::Command, proxy::Proxy},
};
use libp2p::PeerId;
use pyo3::prelude::*;
use tokio::sync::{mpsc, oneshot};
use tracing_subscriber::EnvFilter;

#[pyclass]
#[derive(Default)]
struct PyProxyBuilder {
    center_addr: Option<String>,
    center_peer_id: Option<String>,
    self_addr: Option<String>,
}

#[pymethods]
impl PyProxyBuilder {
    #[new]
    fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    fn center(mut this: PyRefMut<'_, Self>, addr: String, peer_id: String) -> PyRefMut<'_, Self> {
        this.center_addr = Some(addr);
        this.center_peer_id = Some(peer_id);
        this
    }

    fn this(mut this: PyRefMut<'_, Self>, addr: String) -> PyRefMut<'_, Self> {
        this.self_addr = Some(addr);
        this
    }

    fn build(mut this: PyRefMut<'_, Self>) -> PyProxy {
        let center_addr = this.center_addr.take().unwrap().parse().unwrap();
        let center_peer_id = PeerId::from_str(&this.center_peer_id.take().unwrap()).unwrap();
        let self_addr = this.self_addr.take().unwrap().parse().unwrap();
        // let handlers = this.handlers.take().unwrap();

        let (tx, rx) = bounded(1);

        thread::spawn(move || {
            let executor = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(32)
                .enable_all()
                .build()
                .unwrap();
            executor.block_on(async {
                let (proxy, eventloop) = Proxy::new(center_addr, center_peer_id, self_addr);
                tx.send(proxy).unwrap();
                eventloop.run().await;
            });
        });

        let proxy = rx.recv().unwrap();

        proxy.into()
    }
}

#[pyclass]
struct PyProxy {
    peer_id: PeerId,
    command_tx: mpsc::Sender<Command>,
    request_rx: Option<mpsc::Receiver<InboundHandle<codec::Request, codec::Response>>>,
}

impl From<Proxy> for PyProxy {
    fn from(value: Proxy) -> Self {
        Self {
            peer_id: value.peer_id,
            command_tx: value.command_tx,
            request_rx: Some(value.request_rx),
        }
    }
}

#[pymethods]
impl PyProxy {
    fn enable_log(_this: PyRefMut<'_, Self>) {
        let _ = tracing_subscriber::fmt()
            .event_format(
                tracing_subscriber::fmt::format()
                    .with_file(true)
                    .with_line_number(true),
            )
            .with_env_filter(EnvFilter::from_default_env())
            .try_init();
    }

    fn peer_id(this: PyRef<'_, Self>) -> String {
        this.peer_id.to_string()
    }

    fn send(this: PyRefMut<'_, Self>, peer_id: String, request: PyCodecRequest) -> PyCodecResponse {
        let (tx, rx) = oneshot::channel();
        this.command_tx
            .blocking_send(Command::Request {
                peer_id: peer_id.parse().unwrap(),
                request: request.into(),
                sender: tx,
            })
            .unwrap();
        rx.blocking_recv().unwrap().unwrap().into()
    }

    fn recv(this: Py<Self>, py: Python<'_>) -> Option<(PyCodecRequest, PyCallback)> {
        let mut request_rx = this.borrow_mut(py).request_rx.take().unwrap();
        let (request_rx, result) =
            Python::allow_threads(py, move || match request_rx.blocking_recv() {
                Some(handle) => {
                    let (_, request, sender) = handle.into_parts();
                    (request_rx, Some((request.into(), PyCallback::new(sender))))
                }
                None => (request_rx, None),
            });
        this.borrow_mut(py).request_rx = Some(request_rx);

        result
    }
}

#[pyclass]
struct PyCallback {
    sender: Option<oneshot::Sender<codec::Response>>,
}

impl PyCallback {
    pub fn new(sender: oneshot::Sender<codec::Response>) -> Self {
        Self {
            sender: Some(sender),
        }
    }
}

#[pymethods]
impl PyCallback {
    fn send(mut this: PyRefMut<'_, Self>, response: PyCodecResponse) {
        this.sender.take().unwrap().send(response.into()).unwrap();
    }
}

#[pyclass]
#[derive(Clone)]
struct PyCodecRequest {
    #[pyo3(get, set)]
    route: String,
    #[pyo3(get, set)]
    payload: Vec<u8>,
}

#[pymethods]
impl PyCodecRequest {
    #[new]
    fn new(route: String, payload: &[u8]) -> Self {
        Self {
            route,
            payload: Vec::from(payload),
        }
    }
}

impl From<codec::Request> for PyCodecRequest {
    fn from(value: codec::Request) -> Self {
        Self {
            route: value.route,
            payload: value.payload.to_vec(),
        }
    }
}

impl Into<codec::Request> for PyCodecRequest {
    fn into(self) -> codec::Request {
        codec::Request {
            route: self.route,
            payload: Bytes::from(self.payload),
        }
    }
}

#[pyclass]
#[derive(Clone)]
struct PyCodecResponse {
    #[pyo3(get, set)]
    pub status: String,
    #[pyo3(get, set)]
    pub payload: Vec<u8>,
}

#[pymethods]
impl PyCodecResponse {
    #[new]
    #[pyo3(text_signature = "(bytes)")]
    fn new(status: String, payload: &[u8]) -> Self {
        Self {
            status,
            payload: Vec::from(payload),
        }
    }
}

impl From<codec::Response> for PyCodecResponse {
    fn from(value: codec::Response) -> Self {
        Self {
            status: value.status,
            payload: value.payload.to_vec(),
        }
    }
}

impl Into<codec::Response> for PyCodecResponse {
    fn into(self) -> codec::Response {
        codec::Response {
            status: self.status,
            payload: Bytes::from(self.payload),
        }
    }
}

#[pymodule]
fn fleece_network_rust(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyProxy>()?;
    m.add_class::<PyProxyBuilder>()?;
    m.add_class::<PyCallback>()?;
    m.add_class::<PyCodecRequest>()?;
    m.add_class::<PyCodecResponse>()?;
    Ok(())
}
