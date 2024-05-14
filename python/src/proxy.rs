use std::{str::FromStr, thread};

use bytes::Bytes;
use crossbeam_channel::bounded;
use fleece_network::{
    channel::InboundRequestId,
    peer::{codec, eventloop::Command, proxy::Proxy},
};
use libp2p::PeerId;
use pyo3::prelude::*;
use tokio::sync::oneshot;

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
                let (proxy, future) = Proxy::new(center_addr, center_peer_id, self_addr);
                tx.send(proxy).unwrap();
                future.await; // the only way to stall the thread
            });
        });

        let proxy = rx.recv().unwrap();

        PyProxy { inner: proxy }
    }
}

#[pyclass]
struct PyProxy {
    inner: Proxy,
}

#[pymethods]
impl PyProxy {
    fn peer_id(this: PyRef<'_, Self>) -> String {
        this.inner.peer_id.to_string()
    }

    fn send_request(
        this: PyRefMut<'_, Self>,
        peer_id: String,
        request: PyCodecRequest,
    ) -> PyCodecResponse {
        let (tx, rx) = oneshot::channel();
        this.inner
            .command_tx
            .blocking_send(Command::Request {
                peer_id: peer_id.parse().unwrap(),
                request: request.into(),
                sender: tx,
            })
            .unwrap();
        rx.blocking_recv().unwrap().unwrap().into()
    }

    fn send_response(this: PyRefMut<'_, Self>, request_id: PyRequestId, response: PyCodecResponse) {
        let (tx, rx) = oneshot::channel();
        this.inner
            .command_tx
            .blocking_send(Command::Response {
                peer_id: request_id.peer_id,
                request_id: request_id.request_id,
                response: response.into(),
                sender: tx,
            })
            .unwrap();
        rx.blocking_recv().unwrap().unwrap();
    }

    fn recv(this: Py<Self>, py: Python<'_>) -> Option<(PyRequestId, PyCodecRequest)> {
        let message_rx = this.borrow(py).inner.message_rx.clone();
        Python::allow_threads(py, move || match message_rx.recv() {
            Ok((peer_id, request_id, request)) => {
                Some((PyRequestId::new(peer_id, request_id), request.into()))
            }
            Err(_) => None,
        })
    }
}

#[pyclass]
#[derive(Clone)]
struct PyRequestId {
    peer_id: PeerId,
    request_id: InboundRequestId,
}

impl PyRequestId {
    fn new(peer_id: PeerId, request_id: InboundRequestId) -> Self {
        Self {
            peer_id,
            request_id,
        }
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
    m.add_class::<PyRequestId>()?;
    m.add_class::<PyCodecRequest>()?;
    m.add_class::<PyCodecResponse>()?;
    Ok(())
}
