use std::future::Future;
use async_trait::async_trait;

#[async_trait]
pub trait Service<Request> {
    type Response;
    type Error;
    type Future: Future<Output = Result<Self::Response, Self::Error>>;

    async fn ready(&mut self) -> Result<(), Self::Error>;

    fn call(&mut self, req: Request) -> Self::Future;
}

#[async_trait]
pub trait DrivenService<Request> {
    type Response;
    type Error;
    type Future: Future<Output = Result<Self::Response, Self::Error>>;

    async fn ready(&mut self) -> Result<(), Self::Error>;
    async fn drive(&mut self) -> Result<(), Self::Error>;
    async fn close(&mut self) -> Result<(), Self::Error>;

    fn call(&mut self, req: Request) -> Self::Future;
}
