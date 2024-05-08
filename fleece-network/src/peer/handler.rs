use std::collections::HashMap;

use tower::util::BoxService;

use crate::{error::Error, router::Route};

pub struct Handler<T, U> {
    op2services: HashMap<String, BoxService<T, U, Error>>,
}

impl<T, U> Handler<T, U> {
    pub fn new() -> Self {
        Self {
            op2services: Default::default(),
        }
    }

    pub fn add(&mut self, route: String, service: BoxService<T, U, Error>) {
        self.op2services.insert(route, service);
    }
}

impl<'a, T, U> Route<'a, T, U, Error> for Handler<T, U>
where
    T: 'a,
    U: 'a,
{
    fn get(&self, r: &str) -> Result<&BoxService<T, U, Error>, Error> {
        self.op2services.get(r).ok_or(Error::RoutingError)
    }

    fn get_mut(&mut self, r: &str) -> Result<&mut BoxService<T, U, Error>, Error> {
        self.op2services.get_mut(r).ok_or(Error::RoutingError)
    }

    fn services(&'a self) -> impl Iterator<Item = &'a BoxService<T, U, Error>> {
        self.op2services.values()
    }

    fn services_mut(&'a mut self) -> impl Iterator<Item = &'a mut BoxService<T, U, Error>> {
        self.op2services.values_mut()
    }
}
