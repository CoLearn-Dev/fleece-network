use std::{
    future::{ready, Future},
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use tower::{util::BoxService, Service};

pub trait Routable {
    fn route(&self) -> &str;
}

pub trait Route<'a, T: 'a, U: 'a, E: 'a> {
    fn get(&self, r: &str) -> Result<&BoxService<T, U, E>, E>;
    fn get_mut(&mut self, r: &str) -> Result<&mut BoxService<T, U, E>, E>;
    fn services(&'a self) -> impl Iterator<Item = &'a BoxService<T, U, E>>;
    fn services_mut(&'a mut self) -> impl Iterator<Item = &'a mut BoxService<T, U, E>>;
}

pub struct Router<T, U, E, R> {
    route: R,
    _t: PhantomData<T>,
    _u: PhantomData<U>,
    _e: PhantomData<E>,
}

impl<T, U, E, R> Router<T, U, E, R> {
    pub fn new(route: R) -> Self {
        Self {
            route,
            _t: PhantomData,
            _u: PhantomData,
            _e: PhantomData,
        }
    }
}

impl<T, U, E, R> Service<T> for Router<T, U, E, R>
where
    T: Routable,
    U: 'static + Send,
    E: 'static + Send,
    for<'a> R: Route<'a, T, U, E>,
{
    type Response = U;

    type Error = E;

    type Future = Pin<Box<dyn Future<Output = Result<U, E>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        for service in self.route.services_mut() {
            match service.poll_ready(cx) {
                Poll::Ready(_) => continue,
                Poll::Pending => return Poll::Pending,
            }
        }

        return Poll::Ready(Ok(()));
    }

    fn call(&mut self, req: T) -> Self::Future {
        let route = req.route();
        match self.route.get_mut(route) {
            Ok(service) => Box::pin(service.call(req)),
            Err(e) => Box::pin(ready(Err(e))),
        }
    }
}
