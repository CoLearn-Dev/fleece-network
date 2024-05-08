use std::{str::FromStr, thread};

use bytes::Bytes;
use chrono::Local;
use crossbeam_channel::bounded;
use fleece_network::peer::{
    codec,
    proxy::{Instruct, Proxy},
};
use libp2p::{request_response::InboundRequestId, PeerId};
use pyo3::prelude::*;
use tokio::sync::oneshot;

#[pyclass]
#[derive(Default)]
struct PyProxyBuilder {
    center_addr: Option<String>,
    center_peer_id: Option<String>,
    self_addr: Option<String>,
    // handlers: Option<Handler<codec::Request, codec::Response>>,
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

    // fn route(mut this: PyRefMut<'_, Self>, route: String, handler: PyObject) -> PyRefMut<'_, Self> {
    //     let handler = move |req: codec::Request| {
    //         let handler = handler.clone();
    //         async move {
    //             tokio::task::spawn_blocking(move || {
    //                 Python::with_gil(|py| {
    //                     let bytes = PyBytes::new_bound(py, &req.payload);
    //                     let response: PyCodecResponse =
    //                         handler.call1(py, (bytes,)).unwrap().extract(py).unwrap();
    //                     Ok(codec::Response::new(
    //                         response.status,
    //                         Bytes::from(response.payload),
    //                     ))
    //                 })
    //             })
    //             .await
    //             .unwrap()
    //         }
    //     };
    //     if this.handlers.is_none() {
    //         this.handlers = Some(Handler::new());
    //     }
    //     this.handlers
    //         .as_mut()
    //         .unwrap()
    //         .add(route, BoxService::new(service_fn(handler)));

    //     this
    // }

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
        let now = Local::now();
        println!(
            "Current time (microseconds) A: {}",
            now.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
        );
        let (tx, rx) = oneshot::channel();
        this.inner
            .instruct_tx
            .blocking_send(Instruct::Request {
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
            .instruct_tx
            .blocking_send(Instruct::Response {
                request_id: request_id.id,
                response: response.into(),
                sender: tx,
            })
            .unwrap();
        rx.blocking_recv().unwrap().unwrap();
    }

    fn recv(this: Py<Self>, py: Python<'_>) -> Option<(PyRequestId, PyCodecRequest)> {
        let message_rx = this.borrow(py).inner.message_rx.clone();
        Python::allow_threads(py, move || match message_rx.recv() {
            Ok((id, request)) => Some((id.into(), request.into())),
            Err(_) => None,
        })
    }
}

#[pyclass]
#[derive(Clone)]
struct PyRequestId {
    id: InboundRequestId,
}

impl From<InboundRequestId> for PyRequestId {
    fn from(value: InboundRequestId) -> Self {
        Self { id: value }
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
